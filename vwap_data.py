#!/usr/bin/env python3
import os
import time
import requests
import gspread
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from oauth2client.service_account import ServiceAccountCredentials
from pprint import pprint

# -----------------------------
# CONFIG (env-overridable)
# -----------------------------
# Repo workflow sets: SHEET_ID and GOOGLE_CREDENTIALS_PATH
SPREADSHEET_ID = os.environ.get("SHEET_ID", "YOUR_SPREADSHEET_ID")
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDENTIALS_PATH", "path/to/credentials.json")
EXPIRY_DATE = os.environ.get("EXPIRY_DATE", "2025-10-28")

# Networking controls
REQUEST_TIMEOUT_S = float(os.environ.get("REQUEST_TIMEOUT_S", "15"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
BACKOFF_FACTOR = float(os.environ.get("BACKOFF_FACTOR", "0.5"))
REQUEST_DELAY_S = float(os.environ.get("REQUEST_DELAY_S", "0"))  # pacing between symbols

NIFTY50 = [
    "reliance", "tcs", "infy", "hdfcbank", "icicibank", "sbilife", "axisbank",
    "lt", "itc", "sbin", "bhartiartl", "kotakbank", "hcltech", "ongc", "ntpc",
    "techm", "asianpaint", "maruti", "titan", "sunpharma", "hindunilvr", "ultracemco",
    "powergrid", "nestleind", "cipla", "tatamotors", "tatasteel", "coalindia", "drreddy",
    "jswsteel", "grasim", "indusindbk", "bajajfinserv", "bajajfinsv", "hdfclife", "tataconsumer",
    "apollohosp", "britannia", "adaniports", "bpcl", "divislab", "heromotoco", "eichermot",
    "upl", "tvs", "hindalco", "shriramfinance", "suntv", "zomato"
]

HEADERS = [
    'Strike', 'Call OI', 'Call LTP', 'Call IV', 'Call VWAP', 'Call LTP - VWAP',
    'Put OI', 'Put LTP', 'Put IV', 'Put VWAP', 'Put LTP - VWAP',
    'Call Intrinsic', 'Put Intrinsic', 'Abs Diff (Call-Put)', 'Call + Put Diff', 'Spot',
    'Diff Amount (Q)', 'OI Diff (R)', 'R * Call VWAP (S)', 'R * Put VWAP (T)'
]

def _build_session(max_retries: int, backoff_factor: float) -> requests.Session:
    retry = Retry(
        total=max_retries,
        read=max_retries,
        connect=max_retries,
        status=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _authorize_sheets(spreadsheet_id: str, creds_json_path: str) -> gspread.Spreadsheet:
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json_path, scope)
    client = gspread.authorize(creds)
    return client.open_by_key(spreadsheet_id)


session = _build_session(MAX_RETRIES, BACKOFF_FACTOR)
sheet = _authorize_sheets(SPREADSHEET_ID, GOOGLE_CREDS_JSON)

# -----------------------------
# FUNCTION
# -----------------------------
def fetch_option_chain(symbol, index):
    url = f"https://webapi.niftytrader.in/webapi/option/option-chain-data?symbol={symbol}&exchange=nse&expiryDate={EXPIRY_DATE}&atmBelow=0&atmAbove=0"
    headers = {
        "Accept": "application/json",
        "Referer": "https://www.niftytrader.in/",
        "Origin": "https://www.niftytrader.in",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    }
    
    try:
        resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT_S)
    except requests.RequestException as exc:
        print(f"⚠️ Network error for {symbol}: {exc}")
        return

    if resp.status_code != 200:
        print(f"⚠️ HTTP {resp.status_code} for {symbol}")
        return
    
    data_json = resp.json()
    data = data_json.get("resultData", {}).get("opDatas", [])
    if not data:
        print(f"⚠️ No valid data for {symbol}")
        return
    
    # Sheet name
    sheet_name = f"Option_{symbol.upper()}"
    try:
        ws = sheet.worksheet(sheet_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=sheet_name, rows=100, cols=20)
    
    # Prepare rows
    output = [HEADERS]
    call_diff_sum = 0
    put_diff_sum = 0
    
    for item in data:
        callLTP = float(item.get("calls_ltp") or 0)
        callVWAP = float(item.get("calls_average_price") or 0)
        putLTP = float(item.get("puts_ltp") or 0)
        putVWAP = float(item.get("puts_average_price") or 0)
        
        callDiff = callLTP - callVWAP
        putDiff = putLTP - putVWAP
        call_diff_sum += callDiff
        put_diff_sum += putDiff
        
        row = [
            item.get("strike_price") or 0,
            item.get("calls_oi") or 0,
            callLTP,
            item.get("calls_iv") or 0,
            callVWAP,
            callDiff,
            item.get("puts_oi") or 0,
            putLTP,
            item.get("puts_iv") or 0,
            putVWAP,
            putDiff,
            '', '', '', '', '', '', '', '', ''
        ]
        output.append(row)
    
    ws.update("A1", output)
    
    # Spot price
    spot_sheet_name = f"{index+1}.{symbol.upper()}"
    try:
        spot_ws = sheet.worksheet(spot_sheet_name)
        spot = float(spot_ws.acell("A1").value or 0)
    except gspread.WorksheetNotFound:
        spot = 0
        print(f"⚠️ Spot sheet '{spot_sheet_name}' not found.")
    
    # Post-calculation
    calculated_data = []
    for r in output[1:]:
        strike = float(r[0])
        callOI = float(r[1])
        callLTP = float(r[2])
        callVWAP = float(r[4])
        callDiff = float(r[5])
        putOI = float(r[6])
        putLTP = float(r[7])
        putVWAP = float(r[9])
        putDiff = float(r[10])
        
        callIntrinsic = max(spot - strike, 0)
        putIntrinsic = max(strike - spot, 0)
        absDiff = abs(callDiff - putDiff)
        sumDiff = callDiff + putDiff
        q = ((callOI * callLTP) - (putOI * putLTP)) / 10000000
        r_val = (callOI - putOI) / 1000000
        s = r_val * (-callVWAP)
        t = r_val * putVWAP
        
        calculated_data.append([callIntrinsic, putIntrinsic, absDiff, sumDiff, spot, q, r_val, s, t])
    
    # Update post-calculation columns (L to T)
    if calculated_data:
        ws.update("L2", calculated_data)
    
    print(f"✅ {sheet_name} updated. CallDiffSum={call_diff_sum}, PutDiffSum={put_diff_sum}")

def _load_symbols(default_symbols: list[str]) -> list[str]:
    csv = os.environ.get("SYMBOLS", "").strip()
    if not csv:
        return default_symbols
    parsed = [s.strip().lower() for s in csv.split(",") if s.strip()]
    return parsed if parsed else default_symbols


if __name__ == "__main__":
    if SPREADSHEET_ID == "YOUR_SPREADSHEET_ID":
        print("❌ SHEET_ID environment variable not set. Aborting.")
        raise SystemExit(1)

    symbols = _load_symbols(NIFTY50)
    print(f"▶️ Updating {len(symbols)} symbols | expiry={EXPIRY_DATE} | delay={REQUEST_DELAY_S}s")

    for idx, sym in enumerate(symbols):
        fetch_option_chain(sym, idx)
        if REQUEST_DELAY_S > 0 and idx < len(symbols) - 1:
            time.sleep(REQUEST_DELAY_S)

    print("🏁 All NIFTY50 updates completed.")
