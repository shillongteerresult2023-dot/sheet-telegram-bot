"""
bot_monitor.py (Railway production version)
Auto-restart enabled.
Uses SERVICE_ACCOUNT_JSON environment variable.
"""

import asyncio
import re
import os
import json
import sys
import time
from datetime import datetime

from telethon import TelegramClient
import gspread
from google.oauth2.service_account import Credentials

# ---------- CONFIG ----------
API_ID = 36628994
API_HASH = '98bd0303ffbbeb16535e503d830f88fd'
TARGET = '@liveindexbot'

SPREADSHEET_NAME = 'sheet-bot'
SPREADSHEET_ID = None
WORKSHEET_NAME = None

WATCH_COLUMN = 'A'
SENT_COLUMN = 'B'
POLL_INTERVAL = 8
SESSION_NAME = 'telegram_user_session'
# ----------------------------

URL_RE = re.compile(r'(https?://[^\s]+)', re.IGNORECASE)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


def col_idx(letter):
    return ord(letter.upper()) - ord('A') + 1


# ✅ Google Auth via Railway ENV variable
def gsheet_client_from_service_account_json():
    sa_json = os.getenv("SERVICE_ACCOUNT_JSON")
    if not sa_json:
        raise Exception("SERVICE_ACCOUNT_JSON variable not set")

    creds_dict = json.loads(sa_json)
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=SCOPES
    )
    return gspread.authorize(creds)


def open_sheet_sync(gc):
    if SPREADSHEET_ID:
        sh = gc.open_by_key(SPREADSHEET_ID)
    else:
        sh = gc.open(SPREADSHEET_NAME)
    return sh.worksheet(WORKSHEET_NAME) if WORKSHEET_NAME else sh.get_worksheet(0)


async def run_blocking(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)


async def main():

    # Windows compatibility
    if sys.platform.startswith('win'):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    print("✔ Telegram connected")

    try:
        gc = await run_blocking(gsheet_client_from_service_account_json)
        ws = await run_blocking(open_sheet_sync, gc)
    except Exception as e:
        print("Google Sheets error:", e)
        await client.disconnect()
        raise e  # important: allow restart

    watch_idx = col_idx(WATCH_COLUMN)
    sent_idx = col_idx(SENT_COLUMN)

    print(f"Watching sheet '{ws.title}' column {WATCH_COLUMN} → status in {SENT_COLUMN}")

    while True:
        try:
            rows = await run_blocking(ws.get_all_values)

            for r, row in enumerate(rows, start=1):
                try:
                    cell = row[watch_idx-1].strip() if len(row) >= watch_idx else ''
                    status = row[sent_idx-1].strip() if len(row) >= sent_idx else ''

                    if not cell or status.upper().startswith(('SENT', 'SENDING', 'ERROR')):
                        continue

                    m = URL_RE.search(cell)
                    if not m:
                        continue

                    url = m.group(1)

                    await run_blocking(ws.update_cell, r, sent_idx, "SENDING")

                    res = await client.send_message(TARGET, url)
                    t = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    mid = getattr(res, 'id', 'NA')

                    await run_blocking(
                        ws.update_cell,
                        r,
                        sent_idx,
                        f"SENT {t} (msgid:{mid})"
                    )

                    print(f"[SENT] {WATCH_COLUMN}{r}: {url}")

                except Exception as row_err:
                    print(f"Row {r} error:", row_err)

            await asyncio.sleep(POLL_INTERVAL or 8)

        except Exception as loop_error:
            print("Main loop error:", loop_error)
            await asyncio.sleep(5)


# 🔥 AUTO RESTART WRAPPER
if __name__ == '__main__':
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            print("🔥 Bot crashed:", e)
            print("Restarting in 15 seconds...")
            time.sleep(15)
