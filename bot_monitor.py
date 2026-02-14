import asyncio
import re
import os
from datetime import datetime
import sys
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
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']


def col_idx(letter):
    return ord(letter.upper()) - ord('A') + 1


def gsheet_client():
    creds = Credentials.from_service_account_file(
        'service_account.json',
        scopes=SCOPES
    )
    return gspread.authorize(creds)


async def safe_sleep(seconds):
    try:
        await asyncio.sleep(seconds)
    except:
        pass


async def run_bot():

    if sys.platform.startswith('win'):
        try:
            asyncio.set_event_loop_policy(
                asyncio.WindowsSelectorEventLoopPolicy()
            )
        except:
            pass

    client = TelegramClient(
        SESSION_NAME,
        API_ID,
        API_HASH,
        connection_retries=10,
        retry_delay=5
    )

    await client.start()
    print("✔ Telegram connected")

    gc = gsheet_client()

    if SPREADSHEET_ID:
        sh = gc.open_by_key(SPREADSHEET_ID)
    else:
        sh = gc.open(SPREADSHEET_NAME)

    ws = sh.get_worksheet(0) if not WORKSHEET_NAME else sh.worksheet(WORKSHEET_NAME)

    watch_idx = col_idx(WATCH_COLUMN)
    sent_idx = col_idx(SENT_COLUMN)

    print("✔ Bot started successfully")

    while True:
        try:
            rows = ws.get_all_values()

            for r, row in enumerate(rows, start=1):

                cell = row[watch_idx-1].strip() if len(row) >= watch_idx else ''
                status = row[sent_idx-1].strip() if len(row) >= sent_idx else ''

                if not cell or status.upper().startswith(('SENT', 'SENDING', 'ERROR')):
                    continue

                m = URL_RE.search(cell)
                if not m:
                    continue

                url = m.group(1)

                try:
                    ws.update_cell(r, sent_idx, "SENDING")
                except:
                    continue

                try:
                    res = await client.send_message(TARGET, url)
                    t = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    mid = getattr(res, 'id', 'NA')
                    ws.update_cell(r, sent_idx, f"SENT {t} (msgid:{mid})")
                    print(f"[SENT] {url}")
                except Exception as send_err:
                    ws.update_cell(r, sent_idx, f"ERROR {str(send_err)[:100]}")
                    print("Send error:", send_err)

            await safe_sleep(POLL_INTERVAL)

        except Exception as main_error:
            print("Loop crashed:", main_error)
            await safe_sleep(10)


async def main():
    while True:
        try:
            await run_bot()
        except Exception as fatal_error:
            print("Bot crashed completely:", fatal_error)
            print("Restarting in 15 seconds...")
            await asyncio.sleep(15)


if __name__ == '__main__':
    asyncio.run(main())