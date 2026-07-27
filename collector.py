"""
Upstox chart CSV auto-downloader — continuous 5-second polling version.

Logs into Upstox web via username/password + TOTP ONCE, opens the chart for
each instrument, sets the timeframe to 1-second, then loops every 5 seconds
clicking download and appending only new rows to a running master CSV per
instrument. Runs until SESSION_END_IST is reached, then exits cleanly.

Designed to run as one of TWO scheduled jobs per trading day (see the
workflow yaml) because GitHub-hosted runners cap out at 6 hours per job,
and NSE hours (9:15-15:30 IST, 6h15m) exceed that.

BEFORE THIS WORKS, you must fill in the selectors marked with `# FIXME`.
See the bottom of this file for how to find them using Playwright codegen.

Env vars required (set as GitHub Actions secrets/vars):
  UPSTOX_USERNAME     - phone number or client ID used to log in
  UPSTOX_PASSWORD     - login password / PIN
  UPSTOX_TOTP_SECRET  - base32 TOTP secret (same one used to set up your
                        authenticator app; NOT the 6-digit code itself)
  SESSION_END_IST     - "HH:MM" 24h, when this job should stop polling
                        (e.g. "12:25" for the morning job, "15:35" for
                        the afternoon job)
"""

import asyncio
import csv
import os
import sys
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyotp
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ---- Config ---------------------------------------------------------------

UPSTOX_USERNAME = os.environ["UPSTOX_USERNAME"]
UPSTOX_PASSWORD = os.environ["UPSTOX_PASSWORD"]
UPSTOX_TOTP_SECRET = os.environ["UPSTOX_TOTP_SECRET"]
SESSION_END_IST = os.environ.get("SESSION_END_IST", "15:35")

POLL_INTERVAL_SECONDS = 5

# Instruments to download charts for. Add/remove as needed.
INSTRUMENTS = [
    {"name": "nifty", "search_term": "NIFTY 50"},
    {"name": "bank_nifty", "search_term": "NIFTY BANK"},
]

# Master files live in the repo so job 2 can resume from job 1's last row.
MASTER_DIR = Path("data")
MASTER_DIR.mkdir(exist_ok=True)

# Scratch space for each individual 5-second download before we merge it in.
SCRATCH_DIR = Path("scratch")
SCRATCH_DIR.mkdir(exist_ok=True)

LOGIN_URL = "https://pro.upstox.com/"  # FIXME: confirm this is the correct login URL
NAV_TIMEOUT_MS = 30_000
IST = ZoneInfo("Asia/Kolkata")


def session_end_reached() -> bool:
    end_h, end_m = map(int, SESSION_END_IST.split(":"))
    now_ist = datetime.now(IST)
    return now_ist.time() >= dtime(end_h, end_m)


def master_path(instrument_name: str) -> Path:
    today = datetime.now(IST).strftime("%Y-%m-%d")
    return MASTER_DIR / f"{instrument_name}_1s_{today}.csv"


def last_saved_timestamp(instrument_name: str) -> str | None:
    """Read the last row's timestamp from today's master file, if it exists."""
    path = master_path(instrument_name)
    if not path.exists():
        return None
    with open(path, "r", newline="") as f:
        rows = list(csv.reader(f))
    if len(rows) <= 1:  # header only, or empty
        return None
    return rows[-1][0]  # assumes timestamp is the first column — FIXME if not


def append_new_rows(instrument_name: str, new_csv_path: Path):
    """Append rows from a freshly downloaded CSV that are newer than what's
    already in the master file. Assumes timestamp is column 0 and is
    lexically/chronologically sortable (e.g. ISO format)."""
    last_ts = last_saved_timestamp(instrument_name)
    master = master_path(instrument_name)
    write_header = not master.exists()

    with open(new_csv_path, "r", newline="") as f:
        reader = list(csv.reader(f))

    if not reader:
        return 0

    header, rows = reader[0], reader[1:]
    new_rows = [r for r in rows if last_ts is None or r[0] > last_ts]

    with open(master, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(header)
        writer.writerows(new_rows)

    return len(new_rows)


# ---- Core steps -------------------------------------------------------------

async def login(page):
    await page.goto(LOGIN_URL, timeout=NAV_TIMEOUT_MS)

    # FIXME: replace these selectors with the real ones from the login form.
    await page.fill("input[name='username']", UPSTOX_USERNAME)
    await page.click("button:has-text('Continue')")

    await page.fill("input[name='password']", UPSTOX_PASSWORD)
    await page.click("button:has-text('Continue')")

    # TOTP step
    totp_code = pyotp.TOTP(UPSTOX_TOTP_SECRET).now()
    await page.fill("input[name='totp']", totp_code)  # FIXME: real selector
    await page.click("button:has-text('Continue')")

    # Wait for the dashboard/chart shell to load as proof of successful login
    await page.wait_for_selector("text=Watchlist", timeout=NAV_TIMEOUT_MS)  # FIXME


async def open_chart(page, search_term: str):
    # FIXME: replace with the real search box selector
    await page.click("input[placeholder='Search']")
    await page.fill("input[placeholder='Search']", search_term)
    await page.wait_for_timeout(1000)  # let search results render
    await page.click(f"text={search_term}")

    # Wait for chart canvas/container to appear
    await page.wait_for_selector("div.chart-container", timeout=NAV_TIMEOUT_MS)  # FIXME


async def set_timeframe_1s(page):
    # FIXME: real selector for the timeframe dropdown/button
    await page.click("button[data-testid='timeframe-selector']")
    await page.click("text=1 Second")  # FIXME: exact label may differ ("1s", "1S", etc.)
    await page.wait_for_timeout(1500)  # let candles reload


async def download_csv_once(page, instrument_name: str) -> Path:
    """Click download and save the fresh export to scratch (overwritten each poll)."""
    dest = SCRATCH_DIR / f"{instrument_name}_latest.csv"

    # FIXME: real selector for the download/export button (often behind a
    # "..." or export icon on the chart toolbar)
    async with page.expect_download(timeout=NAV_TIMEOUT_MS) as download_info:
        await page.click("button[aria-label='Download data']")

    download = await download_info.value
    await download.save_as(dest)
    return dest


async def poll_instrument_loop(page, instrument: dict):
    """Open the chart once, then poll download+append every 5s until session end."""
    name, search_term = instrument["name"], instrument["search_term"]
    await open_chart(page, search_term)
    await set_timeframe_1s(page)

    poll_count, error_count = 0, 0
    while not session_end_reached():
        loop_start = asyncio.get_event_loop().time()
        try:
            csv_path = await download_csv_once(page, name)
            added = append_new_rows(name, csv_path)
            poll_count += 1
            if poll_count % 60 == 0:  # log roughly every 5 minutes
                print(f"[{name}] poll #{poll_count}, +{added} rows just now")
        except PWTimeout:
            error_count += 1
            print(f"[{name}] download timed out (error #{error_count})")
            if error_count > 20:
                print(f"[{name}] too many consecutive failures, giving up on this instrument.")
                break

        elapsed = asyncio.get_event_loop().time() - loop_start
        await asyncio.sleep(max(0, POLL_INTERVAL_SECONDS - elapsed))

    print(f"[{name}] session end reached, stopping. Total polls: {poll_count}")


# ---- Orchestration ----------------------------------------------------------

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        try:
            await login(page)
        except PWTimeout:
            print("Login failed or selectors are stale. Update the FIXME selectors.")
            await browser.close()
            sys.exit(1)

        # Poll each instrument concurrently, each in its own tab. Tabs share
        # the same browser context, so cookies from the single login above
        # already authenticate every tab — no need to log in again per tab.
        pages = [page] + [await context.new_page() for _ in INSTRUMENTS[1:]]
        for p_ in pages[1:]:
            await p_.goto(LOGIN_URL, timeout=NAV_TIMEOUT_MS)  # loads already-authed session

        await asyncio.gather(*[
            poll_instrument_loop(p_, instrument)
            for p_, instrument in zip(pages, INSTRUMENTS)
        ])

        await browser.close()


if __name__ == "__main__":
    asyncio.run(run())

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
