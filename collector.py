import os
import sys
import time
import logging
import asyncio
from datetime import datetime, timedelta, time as dtime
import queue
import pandas as pd

# Upstox SDK & Streaming Tools
import upstox_client
from upstox_client.feeder import MarketDataStreamerV3

# Google Drive Modules
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- Configuration & Credentials ---
UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")
INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
CSV_FILENAME = "candles_1s_upstox.csv"

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")

# --- Market Hours (NSE, IST) ---
# GitHub Actions runners are always UTC, so IST is computed manually
# (avoids depending on tzdata being installed on the runner).
IST_OFFSET = timedelta(hours=5, minutes=30)
WATCHDOG_CHECK_INTERVAL_SEC = 15

# Session end time is configurable because GitHub-hosted runners hard-cap
# job execution at 360 minutes (6 hours) -- this cannot be raised, even
# with a higher timeout-minutes in the workflow. Since the full market day
# (09:15-15:30 IST) is 375 minutes, one job can't cover it end-to-end, so
# the day is split into two chained sessions (see the workflow yml), each
# passing its own cutoff time via the SESSION_END_IST env var.
_session_end_raw = os.environ.get("SESSION_END_IST", "15:30")
try:
    _hh, _mm = (int(x) for x in _session_end_raw.split(":"))
    SESSION_END_IST = dtime(_hh, _mm)
except (ValueError, TypeError):
    logger.warning(
        f"Could not parse SESSION_END_IST='{_session_end_raw}', defaulting to 15:30."
    )
    SESSION_END_IST = dtime(15, 30)

# Thread-safe handoff between the websocket callback thread (producer,
# via on_message) and the asyncio timer loop (consumer, via
# process_ticks_to_1s). A plain list here is NOT safe: a tick appended by
# the producer between the consumer's "read" and "reset" steps gets
# silently dropped, which is exactly what was causing the high/low
# mismatches against the Upstox chart during fast tick bursts.
tick_queue = queue.Queue()


def process_ticks_to_1s():
    """Drains every tick queued over the past second into one OHLCV bar."""
    ticks = []
    while True:
        try:
            ticks.append(tick_queue.get_nowait())
        except queue.Empty:
            break

    if not ticks:
        return

    now = datetime.now()
    timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

    prices = [t["price"] for t in ticks]
    volumes = [t.get("volume", 0) for t in ticks]

    new_row = {
        "timestamp": timestamp_str,
        "instrument_key": INSTRUMENT_KEY,
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "volume": sum(volumes)
    }

    # Write data row straight to the destination CSV
    df = pd.DataFrame([new_row])
    file_exists = os.path.isfile(CSV_FILENAME)
    df.to_csv(CSV_FILENAME, mode="a", index=False, header=not file_exists)
    logger.info(f"Saved 1s Bar -> {timestamp_str} | Close: {new_row['close']}")


def upload_to_drive():
    if not os.path.exists(CSV_FILENAME):
        return
    if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
        return
    try:
        creds = Credentials(
            token=None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET
        )
        service = build("drive", "v3", credentials=creds)
        query = f"name = '{CSV_FILENAME}' and trashed = false"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        media = MediaFileUpload(CSV_FILENAME, mimetype="text/csv", resumable=True)
        if files:
            file_id = files[0]["id"]
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            file_metadata = {"name": CSV_FILENAME}
            service.files().create(body=file_metadata, media_body=media).execute()
    except Exception as e:
        logger.error(f"Google Drive Sync Failure: {e}")


async def seconds_timer_loop():
    """Triggers the OHLC computation loop precisely at the turn of every clock second."""
    while True:
        now = time.time()
        sleep_time = 1.0 - (now % 1.0)
        await asyncio.sleep(sleep_time)
        process_ticks_to_1s()


async def google_drive_sync_loop():
    while True:
        await asyncio.sleep(10)
        upload_to_drive()


async def market_close_watchdog():
    """
    Polls the current IST time and force-shuts-down the collector once
    SESSION_END_IST is reached. This is what stops the script from running
    on into off-hours if triggered early, started manually, or if a
    previous run overlaps -- and it's what lets one script safely run as
    two chained sub-360-minute sessions to cover the full market day.
    """
    while True:
        now_ist = datetime.utcnow() + IST_OFFSET

        if now_ist.time() >= SESSION_END_IST:
            logger.info(
                f"Session end reached ({SESSION_END_IST.strftime('%H:%M')} IST, "
                f"current IST time {now_ist.strftime('%H:%M:%S')}). Shutting down collector."
            )
            process_ticks_to_1s()   # flush any remaining buffered ticks as a final bar
            upload_to_drive()       # final sync so the day's file is complete on Drive

            # The MarketDataStreamerV3 websocket runs its own (non-daemon)
            # background thread, so a plain sys.exit() would hang here.
            # os._exit() terminates the whole process immediately.
            os._exit(0)

        await asyncio.sleep(WATCHDOG_CHECK_INTERVAL_SEC)


def on_open():
    logger.info("Successfully established connection to Upstox Market Stream Feed.")


def on_message(feed_dict):
    """
    Callback invoked by MarketDataStreamerV3 for every decoded protobuf feed message.
    feed_dict is already a plain dict (via protobuf json_format.MessageToDict).
    """
    try:
        feeds = feed_dict.get("feeds", {})
        feed = feeds.get(INSTRUMENT_KEY)
        if not feed:
            return

        full_feed = feed.get("fullFeed", {})

        # NSE_INDEX instruments populate indexFF, not marketFF (indices have no
        # traded-quantity/market-depth fields the way equities/derivatives do).
        ltpc = full_feed.get("indexFF", {}).get("ltpc") \
            or full_feed.get("marketFF", {}).get("ltpc") \
            or feed.get("ltpc")

        if not ltpc:
            return

        ltp = ltpc.get("ltp")
        ltq = ltpc.get("ltq", 0)

        if ltp is not None:
            tick_queue.put_nowait({
                "price": float(ltp),
                "volume": float(ltq)
            })
    except Exception as e:
        logger.error(f"Error reading live feed update: {e}")


def on_error(error):
    logger.error(f"Upstox WebSocket Feed Error: {error}")


def on_close(close_status_code, close_msg):
    logger.info(f"WebSocket connection closed. Code: {close_status_code}, Msg: {close_msg}")


def start_streamer(token):
    """Configures and connects the MarketDataStreamerV3 (non-blocking; runs on its own thread)."""
    configuration = upstox_client.Configuration()
    configuration.access_token = token
    api_client = upstox_client.ApiClient(configuration)

    streamer = MarketDataStreamerV3(api_client, [INSTRUMENT_KEY], "full")
    streamer.on("open", on_open)
    streamer.on("message", on_message)
    streamer.on("error", on_error)
    streamer.on("close", on_close)

    streamer.connect()  # spawns its own background thread; returns immediately
    return streamer


async def main():
    if not UPSTOX_ACCESS_TOKEN:
        logger.error("UPSTOX_ACCESS_TOKEN configuration secret is missing!")
        sys.exit(1)

    start_streamer(UPSTOX_ACCESS_TOKEN)

    logger.info("Starting up active 1s real-time loops...")
    await asyncio.gather(
        seconds_timer_loop(),
        google_drive_sync_loop(),
        market_close_watchdog(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script stopped manually by user.")
