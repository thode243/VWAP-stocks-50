# -----------------------------------
# SENSEX Option Chain Updater
# (KiteConnect + Google Sheets)
# -----------------------------------

import os
import sys
import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from kiteconnect import KiteConnect
from datetime import datetime, time
from gspread.exceptions import WorksheetNotFound

# -----------------------------------
# CONFIGURATION
# -----------------------------------
SHEET_ID = os.getenv("SHEET_ID")
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "service_account.json")
API_KEY = os.getenv("API_KEY")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# Define expiry sheet mappings
EXPIRIES = [
    ("2025-10-23", "SENSEX_Exp_1"),  # (expiry_date, sheet_tab_name)
]

# -----------------------------------
# MARKET TIME CHECK (IST)
# -----------------------------------
ist = pytz.timezone("Asia/Kolkata")
now = datetime.now(ist)
current_time = now.time()
market_open = time(8, 40)
market_close = time(15, 30)

if not (market_open <= current_time <= market_close) or now.weekday() >= 5:
    print("🕒 Market is closed, exiting script.")
    sys.exit(0)
print(f"✅ Market is open — Time: {current_time}")

# -----------------------------------
# KITE CONNECT SETUP
# -----------------------------------
if not API_KEY or not ACCESS_TOKEN:
    raise Exception("❌ Missing API_KEY or ACCESS_TOKEN in environment!")

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# -----------------------------------
# GOOGLE SHEETS SETUP
# -----------------------------------
if not SHEET_ID or not os.path.exists(GOOGLE_CREDS_PATH):
    raise Exception("❌ Missing Google Sheet ID or credentials file!")

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_PATH, scope)
client = gspread.authorize(creds)
spreadsheet = client.open_by_key(SHEET_ID)

# -----------------------------------
# MAIN LOOP — PER EXPIRY
# -----------------------------------
for expiry, sheet_name in EXPIRIES:
    print(f"\n📘 Processing expiry {expiry} → Sheet '{sheet_name}'")

    try:
        # Try getting worksheet, or create if missing
        try:
            sheet = spreadsheet.worksheet(sheet_name)
        except WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
            print(f"🆕 Created new worksheet: {sheet_name}")

        # Fetch previous OI data
        existing_values = sheet.get_all_values()
        prev_oi_dict = {}
        if existing_values:
            headers = existing_values[0]
            if all(h in headers for h in ["Strike", "Call OI", "Put OI"]):
                strike_col = headers.index("Strike")
                call_oi_col = headers.index("Call OI")
                put_oi_col = headers.index("Put OI")
                for row in existing_values[1:]:
                    try:
                        strike = float(row[strike_col])
                        call_oi = int(row[call_oi_col]) if row[call_oi_col] else 0
                        put_oi = int(row[put_oi_col]) if row[put_oi_col] else 0
                        prev_oi_dict[strike] = {"call": call_oi, "put": put_oi}
                    except Exception:
                        continue

        # -----------------------------------
        # FETCH SENSEX OPTION CHAIN
        # -----------------------------------
        instruments = kite.instruments("BFO")  # SENSEX options are in BFO
        sensex_options = [
            i for i in instruments
            if i["name"] == "SENSEX" and i["expiry"].strftime("%Y-%m-%d") == expiry
        ]

        print(f"📊 Found {len(sensex_options)} SENSEX contracts for {expiry}")

        option_chain = {}
        for inst in sensex_options:
            try:
                quote = kite.quote(inst["instrument_token"])
                data = quote[str(inst["instrument_token"])]
                ltp = data.get("last_price", 0)
                oi = data.get("oi", 0)
                vol = data.get("volume", 0)
                strike = inst["strike"]
                opt_type = inst["instrument_type"]  # CE or PE

                if strike not in option_chain:
                    option_chain[strike] = {"call": {}, "put": {}}

                prev_oi = prev_oi_dict.get(strike, {}).get(
                    "call" if opt_type == "CE" else "put", 0
                )

                option_chain[strike]["call" if opt_type == "CE" else "put"] = {
                    "ltp": ltp,
                    "oi": oi,
                    "chg_oi": oi - prev_oi,
                    "vol": vol,
                }

            except Exception as e:
                print(f"⚠️ Error fetching {inst['tradingsymbol']}: {e}")

        # -----------------------------------
        # WRITE TO GOOGLE SHEET
        # -----------------------------------
        headers_row = [
            "Call LTP", "Call OI", "Call Chg OI", "Call Vol",
            "Strike", "Expiry",
            "Put LTP", "Put OI", "Put Chg OI", "Put Vol",
            "VWAP"
        ]

        rows = []
        for strike, data in sorted(option_chain.items()):
            call = data.get("call", {})
            put = data.get("put", {})
            rows.append([
                call.get("ltp", 0),
                call.get("oi", 0),
                call.get("chg_oi", 0),
                call.get("vol", 0),
                strike,
                expiry,
                put.get("ltp", 0),
                put.get("oi", 0),
                put.get("chg_oi", 0),
                put.get("vol", 0),
                ""  # VWAP placeholder
            ])

        sheet.clear()
        sheet.insert_row(headers_row, 1)
        if rows:
            sheet.insert_rows(rows, 2)

        print(f"✅ Logged {len(rows)} rows in {sheet_name}")

    except Exception as e:
        print(f"❌ Error processing {expiry}: {e}")
