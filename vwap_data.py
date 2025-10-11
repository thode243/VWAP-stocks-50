#!/usr/bin/env python3
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pprint import pprint

# -----------------------------
# CONFIG
# -----------------------------
SPREADSHEET_ID = "YOUR_SPREADSHEET_ID"  # Replace with your sheet ID
GOOGLE_CREDS_JSON = "path/to/credentials.json"  # Service account JSON
EXPIRY_DATE = "2025-10-28"

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

# -----------------------------
# GOOGLE SHEETS AUTH
# -----------------------------
scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_JSON, scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(SPREADSHEET_ID)

# -----------------------------
# FUNCTION
# -----------------------------
def fetch_option_chain(symbol, index):
    url = f"https://webapi.niftytrader.in/webapi/option/option-chain-data?symbol={symbol}&exchange=nse&expiryDate={EXPIRY_DATE}&atmBelow=0&atmAbove=0"
    headers = {
        "Accept": "application/json",
        "Referer": "https://www.niftytrader.in/",
        "Origin": "https://www.niftytrader.in",
        "User-Agent": "Mozilla/5.0"
    }
    
    resp = requests.get(url, headers=headers)
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
        ws.update(f"L2", calculated_data)
    
    print(f"✅ {sheet_name} updated. CallDiffSum={call_diff_sum}, PutDiffSum={put_diff_sum}")

# -----------------------------
# MAIN LOOP
# -----------------------------
for idx, sym in enumerate(NIFTY50):
    fetch_option_chain(sym, idx)

print("🏁 All NIFTY50 updates completed.")
