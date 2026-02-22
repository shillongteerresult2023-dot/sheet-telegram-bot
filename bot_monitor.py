import asyncio
import re
from datetime import datetime

from telethon import TelegramClient
import gspread
from google.oauth2.service_account import Credentials

# ========= CONFIG =========
API_ID = 38385459
API_HASH = '5bcf1de656a19ab7181ed6b04c07c986'
TARGET = '@liveindexbot'

SPREADSHEET_NAME = 'sheet-bot'
FORM_SHEET_NAME = 'Form_Responses'
LIMIT_SHEET_NAME = 'Limits'

WATCH_COLUMN = 'A'
STATUS_COLUMN = 'B'
EMAIL_COLUMN = 'C'

POLL_INTERVAL = 8
SESSION_NAME = 'telegram_user_session'
# ===========================

URL_RE = re.compile(r'(https?://[^\s,]+)', re.IGNORECASE)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def col_idx(letter):
    return ord(letter.upper()) - ord('A') + 1

def gsheet_client():
    creds = Credentials.from_service_account_file(
        'service_account.json', scopes=SCOPES)
    return gspread.authorize(creds)

async def run_blocking(func, *args):
    return await asyncio.to_thread(func, *args)

async def main():

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    print("✔ Telegram connected")

    gc = await run_blocking(gsheet_client)

    spreadsheet = gc.open(SPREADSHEET_NAME)

    print("Available sheets:")
    for s in spreadsheet.worksheets():
        print(" -", s.title)

    form_ws = spreadsheet.worksheet(FORM_SHEET_NAME)
    limit_ws = spreadsheet.worksheet(LIMIT_SHEET_NAME)

    watch_idx = col_idx(WATCH_COLUMN)
    status_idx = col_idx(STATUS_COLUMN)
    email_idx = col_idx(EMAIL_COLUMN)

    while True:

        rows = await run_blocking(form_ws.get_all_values)

        for r, row in enumerate(rows, start=1):

            if len(row) < email_idx:
                continue

            cell = row[watch_idx-1].strip()
            status = row[status_idx-1].strip() if len(row) >= status_idx else ''

            # ONLY skip SENT & ERROR (allow retry if stuck on SENDING)
            if not cell or status.startswith(('SENT', 'ERROR')):
                continue

            urls = URL_RE.findall(cell)
            if not urls:
                continue

            email = row[email_idx-1].strip()

            limits = await run_blocking(limit_ws.get_all_values)

            limit_row_number = None
            for lr, lrow in enumerate(limits, start=1):
                if lrow and lrow[0].strip().lower() == email.lower():
                    limit_row_number = lr
                    break

            if not limit_row_number:
                print("Email not found:", email)
                continue

            limit_value = int(limits[limit_row_number-1][1])

            # SAFE used value
            used_raw = limits[limit_row_number-1][2] if len(limits[limit_row_number-1]) > 2 else "0"
            try:
                used_value = int(used_raw)
            except:
                used_value = 0

            remaining = limit_value - used_value

            print("Email:", email)
            print("Total URLs:", len(urls))
            print("Remaining:", remaining)

            if remaining <= 0:
                await run_blocking(form_ws.update_cell, r, status_idx, "LIMIT REACHED")
                continue

            allowed_urls = urls[:remaining]

            await run_blocking(form_ws.update_cell, r, status_idx, "SENDING")

            sent_count = 0

            for url in allowed_urls:
                try:
                    print("Sending:", url)
                    await asyncio.wait_for(
                        client.send_message(TARGET, url),
                        timeout=20
                    )
                    sent_count += 1
                    await asyncio.sleep(2)
                except Exception as e:
                    print("Send error:", e)

            # Update Used correctly
            new_used = used_value + sent_count
            await run_blocking(limit_ws.update_cell, limit_row_number, 3, new_used)

            t = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            await run_blocking(
                form_ws.update_cell,
                r,
                status_idx,
                f"SENT {t} ({sent_count} links)"
            )

        await asyncio.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    asyncio.run(main())
