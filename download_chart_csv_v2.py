"""
Upstox chart CSV auto-downloader — browser-UI based, continuous 5-second
polling, no WebSocket/API involved.

Logs into Upstox web via username/password + TOTP ONCE per session, opens
the chart for each instrument, sets the timeframe to 1-second, then loops
every 5 seconds clicking the chart's download button and appending only
new rows to a running master CSV per instrument. Runs until SESSION_END_IST
is reached, then exits cleanly.

Designed to run as one of TWO sessions per trading day (see the workflow
yaml) because GitHub-hosted runners cap out at 360 minutes per job, and
NSE hours (9:15-15:30 IST, 6h15m) exceed that.

BEFORE THIS WORKS, you must fill in the selectors marked with `# FIXME`.
See the bottom of this file for how to find them using Playwright codegen.

Env vars required (set as GitHub Actions secrets):
  UPSTOX_USERNAME      - phone number or client ID used to log in
  UPSTOX_PASSWORD      - login password / PIN
  UPSTOX_TOTP_SECRET   - base32 TOTP secret (same one used to set up your
                         authenticator app; NOT the 6-digit code itself)
  SESSION_END_IST      - "HH:MM" 24h, when this job should stop polling
  GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET / GOOGLE_REFRESH_TOKEN
                       - same Drive credentials your WebSocket collector uses
"""

import asyncio
import csv
import io
import logging
import os
import sys
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyotp
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials

# --- Logging Setup (same pattern as collector.py) ---
sys.stdout.reconfigure(line_buffering=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

# ---- Config ---------------------------------------------------------------

UPSTOX_USERNAME = os.environ["UPSTOX_USERNAME"]
UPSTOX_PASSWORD = os.environ["UPSTOX_PASSWORD"]
UPSTOX_TOTP_SECRET = os.environ["UPSTOX_TOTP_SECRET"]
SESSION_END_IST = os.environ.get("SESSION_END_IST", "15:35")

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")

POLL_INTERVAL_SECONDS = 5

# Instruments to download charts for. label drives every filename below,
# same convention as collector.py (nifty_2026-07-27.csv, etc).
INSTRUMENTS = [
    {"label": "nifty", "search_term": "NIFTY 50"},
    {"label": "bank_nifty", "search_term": "NIFTY BANK"},
]

# Folder layout mirrors collector.py:
#   data/
#     daily/
#       2026-07-27/nifty_chart_2026-07-27.csv
#       2026-07-27/bank_nifty_chart_2026-07-27.csv
#     nifty_chart_ALL.csv
#     bank_nifty_chart_ALL.csv
BASE_DATA_DIR = Path("data")
DAILY_DIR = BASE_DATA_DIR / "daily"
SCRATCH_DIR = Path("scratch")  # holds each raw 5-second download before merging
SCRATCH_DIR.mkdir(exist_ok=True)

LOGIN_URL = "https://pro.upstox.com/"  # FIXME: confirm this is the correct login URL
NAV_TIMEOUT_MS = 30_000


def get_daily_path(label: str, date_str: str) -> Path:
    return DAILY_DIR / date_str / f"{label}_chart_{date_str}.csv"


def get_combined_filename(label: str) -> str:
    return f"{label}_chart_ALL.csv"


def get_combined_path(label: str) -> Path:
    return BASE_DATA_DIR / get_combined_filename(label)


def session_end_reached() -> bool:
    end_h, end_m = map(int, SESSION_END_IST.split(":"))
    return datetime.now(IST).time() >= dtime(end_h, end_m)


def last_saved_timestamp(label: str, date_str: str) -> str | None:
    """Read the last row's timestamp from today's daily file, if it exists."""
    path = get_daily_path(label, date_str)
    if not path.exists():
        return None
    with open(path, "r", newline="") as f:
        rows = list(csv.reader(f))
    if len(rows) <= 1:
        return None
    return rows[-1][0]  # assumes timestamp is column 0 — FIXME if the real export differs


def append_new_rows(label: str, new_csv_path: Path) -> int:
    """Append rows from a freshly downloaded CSV that are newer than what's
    already saved today, to BOTH the daily file and the combined file —
    same dual-write pattern as collector.py's write_bar()."""
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    last_ts = last_saved_timestamp(label, today_str)

    with open(new_csv_path, "r", newline="") as f:
        reader = list(csv.reader(f))
    if not reader:
        return 0

    header, rows = reader[0], reader[1:]
    new_rows = [r for r in rows if last_ts is None or r[0] > last_ts]
    if not new_rows:
        return 0

    for path in (get_daily_path(label, today_str), get_combined_path(label)):
        path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not path.exists()
        with open(path, "a", newline="") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(header)
            writer.writerows(new_rows)

    return len(new_rows)


# ---- Google Drive sync (same functions/approach as collector.py) ----------

def _get_drive_service():
    creds = Credentials(
        token=None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET
    )
    return build("drive", "v3", credentials=creds)


def get_or_create_drive_folder(service, name: str, parent_id: str = None) -> str:
    query = f"mimeType = 'application/vnd.google-apps.folder' and name = '{name}' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    results = service.files().list(q=query, fields="files(id)").execute()
    folders = results.get("files", [])
    if folders:
        return folders[0]["id"]
    metadata = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        metadata["parents"] = [parent_id]
    folder = service.files().create(body=metadata, fields="id").execute()
    logger.info(f"Created Drive folder '{name}'" + (f" inside parent {parent_id}" if parent_id else ""))
    return folder["id"]


def get_daily_drive_folder_id(service) -> str:
    date_str = datetime.now(IST).strftime("%Y-%m-%d")
    daily_folder_id = get_or_create_drive_folder(service, "daily")
    return get_or_create_drive_folder(service, date_str, parent_id=daily_folder_id)


def _is_combined_filename(drive_filename: str) -> bool:
    return drive_filename in {get_combined_filename(i["label"]) for i in INSTRUMENTS}


def upload_file_to_drive(local_path: Path, drive_filename: str, parent_id: str = None):
    if not local_path.exists():
        return
    if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
        return
    try:
        service = _get_drive_service()
        if parent_id is None and not _is_combined_filename(drive_filename):
            parent_id = get_daily_drive_folder_id(service)

        query = f"name = '{drive_filename}' and trashed = false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        media = MediaFileUpload(str(local_path), mimetype="text/csv", resumable=True)
        if files:
            service.files().update(fileId=files[0]["id"], media_body=media).execute()
        else:
            file_metadata = {"name": drive_filename}
            if parent_id:
                file_metadata["parents"] = [parent_id]
            service.files().create(body=file_metadata, media_body=media).execute()
    except Exception as e:
        logger.error(f"Google Drive Sync Failure ({drive_filename}): {e}")


def download_file_from_drive(drive_filename: str, local_path: Path, parent_id: str = None):
    """Pulls existing Drive history down at startup, same reasoning as
    collector.py: GitHub Actions runners start with an empty disk each run."""
    if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
        return
    try:
        service = _get_drive_service()
        if parent_id is None and not _is_combined_filename(drive_filename):
            parent_id = get_daily_drive_folder_id(service)

        query = f"name = '{drive_filename}' and trashed = false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        if not files:
            return
        local_path.parent.mkdir(parents=True, exist_ok=True)
        request = service.files().get_media(fileId=files[0]["id"])
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        with open(local_path, "wb") as f:
            f.write(buf.getvalue())
        logger.info(f"Resumed existing '{drive_filename}' from Drive -> {local_path}")
    except Exception as e:
        logger.error(f"Google Drive Resume/Download Failure ({drive_filename}): {e}")


async def google_drive_sync_loop():
    """Runs alongside the polling loops, syncing every 10s — same interval
    and to_thread pattern as collector.py, so a slow upload can never stall
    the 5-second click cadence."""
    while True:
        await asyncio.sleep(10)
        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        for instrument in INSTRUMENTS:
            label = instrument["label"]
            daily_path = get_daily_path(label, today_str)
            await asyncio.to_thread(upload_file_to_drive, daily_path, daily_path.name)
            await asyncio.to_thread(upload_file_to_drive, get_combined_path(label), get_combined_filename(label))


# ---- Browser steps ----------------------------------------------------------

async def login(page):
    await page.goto(LOGIN_URL, timeout=NAV_TIMEOUT_MS)

    # FIXME: replace these selectors with the real ones from the login form.
    await page.fill("input[name='username']", UPSTOX_USERNAME)
    await page.click("button:has-text('Continue')")

    await page.fill("input[name='password']", UPSTOX_PASSWORD)
    await page.click("button:has-text('Continue')")

    totp_code = pyotp.TOTP(UPSTOX_TOTP_SECRET).now()
    await page.fill("input[name='totp']", totp_code)  # FIXME: real selector
    await page.click("button:has-text('Continue')")

    await page.wait_for_selector("text=Watchlist", timeout=NAV_TIMEOUT_MS)  # FIXME
    logger.info("Successfully logged into Upstox web.")


async def open_chart(page, search_term: str):
    # FIXME: replace with the real search box selector
    await page.click("input[placeholder='Search']")
    await page.fill("input[placeholder='Search']", search_term)
    await page.wait_for_timeout(1000)
    await page.click(f"text={search_term}")
    await page.wait_for_selector("div.chart-container", timeout=NAV_TIMEOUT_MS)  # FIXME


async def set_timeframe_1s(page):
    # FIXME: real selector for the timeframe dropdown/button
    await page.click("button[data-testid='timeframe-selector']")
    await page.click("text=1 Second")  # FIXME: exact label may differ
    await page.wait_for_timeout(1500)


async def download_csv_once(page, label: str) -> Path:
    dest = SCRATCH_DIR / f"{label}_latest.csv"
    # FIXME: real selector for the download/export button
    async with page.expect_download(timeout=NAV_TIMEOUT_MS) as download_info:
        await page.click("button[aria-label='Download data']")
    download = await download_info.value
    await download.save_as(dest)
    return dest


async def poll_instrument_loop(page, instrument: dict):
    label, search_term = instrument["label"], instrument["search_term"]
    await open_chart(page, search_term)
    await set_timeframe_1s(page)
    logger.info(f"[{label}] Chart ready, starting 5s poll loop until {SESSION_END_IST} IST.")

    poll_count, error_count = 0, 0
    while not session_end_reached():
        loop_start = asyncio.get_event_loop().time()
        try:
            csv_path = await download_csv_once(page, label)
            added = append_new_rows(label, csv_path)
            poll_count += 1
            error_count = 0
            if poll_count % 60 == 0:  # roughly every 5 minutes
                logger.info(f"[{label}] poll #{poll_count}, +{added} rows just now")
        except PWTimeout:
            error_count += 1
            logger.warning(f"[{label}] download timed out (consecutive failure #{error_count})")
            if error_count > 20:
                logger.error(f"[{label}] too many consecutive failures, giving up on this instrument.")
                break

        elapsed = asyncio.get_event_loop().time() - loop_start
        await asyncio.sleep(max(0, POLL_INTERVAL_SECONDS - elapsed))

    logger.info(f"[{label}] session end reached, stopping. Total polls: {poll_count}")


# ---- Orchestration ----------------------------------------------------------

async def run():
    os.makedirs(BASE_DATA_DIR, exist_ok=True)

    # Resume existing history from Drive first, same reasoning as collector.py.
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    for instrument in INSTRUMENTS:
        label = instrument["label"]
        daily_path = get_daily_path(label, today_str)
        await asyncio.to_thread(download_file_from_drive, daily_path.name, daily_path)
        await asyncio.to_thread(download_file_from_drive, get_combined_filename(label), get_combined_path(label))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        try:
            await login(page)
        except PWTimeout:
            logger.error("Login failed or selectors are stale. Update the FIXME selectors.")
            await browser.close()
            sys.exit(1)

        # Each instrument polls in its own tab; tabs share the context's
        # cookies, so only one login is needed for the whole browser.
        pages = [page] + [await context.new_page() for _ in INSTRUMENTS[1:]]
        for p_ in pages[1:]:
            await p_.goto(LOGIN_URL, timeout=NAV_TIMEOUT_MS)

        await asyncio.gather(
            google_drive_sync_loop(),
            *[poll_instrument_loop(p_, instrument) for p_, instrument in zip(pages, INSTRUMENTS)],
        )

        await browser.close()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Script stopped manually by user.")

# -----------------------------------------------------------------------------
# HOW TO FIND THE REAL SELECTORS (do this once, locally, with a real browser):
#
#   pip install playwright
#   playwright install chromium
#   playwright codegen https://pro.upstox.com/
#
# This opens a browser + an "Inspector" window. Log in and click through to
# the chart, set the timeframe to 1 second, and click download — Playwright
# will auto-generate the exact selectors for every click/fill you make in the
# Inspector panel. Copy those into the FIXME lines above.
# -----------------------------------------------------------------------------
