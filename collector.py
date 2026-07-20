import os
import sys
import time
import logging
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd

# Upstox SDK & Streaming Tools
import upstox_client
from upstox_client.feeder import MarketDataStreamerV3

# Google Drive Modules
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- Timezone ---
IST = ZoneInfo("Asia/Kolkata")

# --- Configuration & Credentials ---
UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")
INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"

# Folder layout:
#   data/
#     daily/
#       2026-07-20/candles_1s_upstox_2026-07-20.csv   <- one folder+file per day
#       2026-07-21/candles_1s_upstox_2026-07-21.csv
#     candles_1s_upstox_ALL.csv                        <- every day's rows combined
BASE_DATA_DIR = "data"
DAILY_DIR = os.path.join(BASE_DATA_DIR, "daily")
COMBINED_FILENAME = "candles_1s_upstox_ALL.csv"
COMBINED_PATH = os.path.join(BASE_DATA_DIR, COMBINED_FILENAME)


def get_daily_path(date_str: str) -> str:
    """Returns the per-day CSV path, always nested inside DAILY_DIR,
    e.g. data/daily/2026-07-20/candles_1s_upstox_2026-07-20.csv"""
    return os.path.join(DAILY_DIR, date_str, f"candles_1s_upstox_{date_str}.csv")


GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")

# Ticks are bucketed by their OWN exchange timestamp (ltt from Upstox's feed),
# NOT by local arrival time. Arrival time is skewed by network/processing
# jitter, which is exactly what caused OHLC values to mismatch Upstox's own
# per-second data for a bar with the same label. Key = epoch second (int).
tick_buckets = {}

# How many seconds we hold a bucket open after its second has technically
# elapsed, before finalizing/writing it. This exists ONLY to give slightly
# late-arriving ticks (still stamped with the correct ltt) time to land in
# the right bucket. Larger = more accurate vs Upstox, smaller = less delay.
BUFFER_SECONDS = 2

# The last epoch-second we've already finalized (written or logged as a
# gap). Used to walk forward one second at a time so no second is ever
# silently skipped, whether the broker sent nothing or the process stalled.
last_flushed_epoch = None


def append_row_to_csv(path: str, row: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = pd.DataFrame([row])
    file_exists = os.path.isfile(path)
    df.to_csv(path, mode="a", index=False, header=not file_exists)


def write_bar(bar_time: datetime, ticks: list):
    """Writes one finalized 1s OHLCV bar built entirely from ticks whose own
    exchange timestamp (ltt) falls in this second -- to BOTH that day's
    dedicated file and the running all-days combined file."""
    bar_str = bar_time.strftime("%Y-%m-%d %H:%M:%S")
    date_str = bar_time.strftime("%Y-%m-%d")
    tick_count = len(ticks)
    prices = [t["price"] for t in ticks]
    volumes = [t.get("volume", 0) for t in ticks]

    new_row = {
        "timestamp": bar_str,
        "instrument_key": INSTRUMENT_KEY,
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "volume": sum(volumes)
    }

    append_row_to_csv(get_daily_path(date_str), new_row)
    append_row_to_csv(COMBINED_PATH, new_row)
    logger.info(f"Saved 1s Bar -> {bar_str} IST | Close: {new_row['close']} | ticks: {tick_count}")


def flush_ready_buckets():
    """Walks forward second-by-second from the last finalized second up to
    (now - BUFFER_SECONDS), writing or gap-logging each one. Ticks arriving
    late for an already-finalized second are impossible by construction,
    since we never finalize a second until BUFFER_SECONDS have passed."""
    global tick_buckets, last_flushed_epoch

    now_epoch = int(time.time())
    cutoff = now_epoch - BUFFER_SECONDS  # newest second considered "settled"

    if last_flushed_epoch is None:
        # First run: don't dump/backfill history, just start the walk from here.
        last_flushed_epoch = cutoff - 1
        return

    for epoch_sec in range(last_flushed_epoch + 1, cutoff + 1):
        bar_time = datetime.fromtimestamp(epoch_sec, tz=IST)
        bar_str = bar_time.strftime("%Y-%m-%d %H:%M:%S")
        ticks = tick_buckets.pop(epoch_sec, None)
        if not ticks:
            logger.warning(f"NO TICKS received for bar {bar_str} IST - broker delivery gap, bar skipped")
            continue
        write_bar(bar_time, ticks)

    last_flushed_epoch = cutoff



def _get_drive_service():
    creds = Credentials(
        token=None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET
    )
    return build("drive", "v3", credentials=creds)


def upload_file_to_drive(local_path: str, drive_filename: str):
    """Uploads/replaces a single named file on Drive (flat namespace -- Drive
    itself has no subfolders here, only the local disk is split by day)."""
    if not os.path.exists(local_path):
        return
    if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
        return
    try:
        service = _get_drive_service()
        query = f"name = '{drive_filename}' and trashed = false"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
        if files:
            file_id = files[0]["id"]
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            file_metadata = {"name": drive_filename}
            service.files().create(body=file_metadata, media_body=media).execute()
    except Exception as e:
        logger.error(f"Google Drive Sync Failure ({drive_filename}): {e}")


def download_file_from_drive(drive_filename: str, local_path: str):
    """Pulls an existing Drive file down to local_path if one exists. Used at
    startup so history isn't lost -- GitHub Actions runners start with an
    empty disk every run, so without this, each new run would overwrite the
    combined file (and a same-day restart would overwrite that day's file)
    with just the current run's data instead of adding to it."""
    if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
        return
    try:
        service = _get_drive_service()
        query = f"name = '{drive_filename}' and trashed = false"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        if not files:
            return
        file_id = files[0]["id"]
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        request = service.files().get_media(fileId=file_id)
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


async def seconds_timer_loop():
    """Periodically triggers flush_ready_buckets(). Wake-time precision no
    longer matters for correctness -- bars are labeled and bucketed using
    each tick's own exchange timestamp (ltt), not this loop's timing. This
    loop just needs to run roughly once a second so buckets don't pile up."""
    while True:
        now = time.time()
        sleep_time = 1.0 - (now % 1.0)
        await asyncio.sleep(sleep_time)
        flush_ready_buckets()


async def google_drive_sync_loop():
    while True:
        await asyncio.sleep(10)
        # Both uploads are blocking HTTP calls -- run each on a worker thread
        # so they can never freeze seconds_timer_loop (that's what caused
        # entire 1s bars to go missing right after every sync, before).
        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        daily_path = get_daily_path(today_str)
        daily_drive_name = os.path.basename(daily_path)
        await asyncio.to_thread(upload_file_to_drive, daily_path, daily_drive_name)
        await asyncio.to_thread(upload_file_to_drive, COMBINED_PATH, COMBINED_FILENAME)


def on_open():
    logger.info("Successfully established connection to Upstox Market Stream Feed.")


def on_message(feed_dict):
    """
    Callback invoked by MarketDataStreamerV3 for every decoded protobuf feed message.
    feed_dict is already a plain dict (via protobuf json_format.MessageToDict).
    """
    global tick_buckets
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
        ltt = ltpc.get("ltt")  # exchange timestamp of this trade, epoch millis

        if ltp is None:
            return

        # Bucket by the tick's OWN exchange second, not local receipt time.
        if ltt is not None:
            try:
                tick_epoch_sec = int(ltt) // 1000
            except (TypeError, ValueError):
                tick_epoch_sec = int(time.time())
                logger.warning(f"ltt field unparseable ({ltt!r}) - falling back to local arrival time for this tick")
        else:
            tick_epoch_sec = int(time.time())
            logger.warning("ltt field missing from tick - falling back to local arrival time for this tick")

        tick_buckets.setdefault(tick_epoch_sec, []).append({
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

    os.makedirs(BASE_DATA_DIR, exist_ok=True)

    # Resume any existing history from Drive before appending anything new.
    # Without this, a fresh GitHub Actions runner would locally start both
    # files empty and the next sync would overwrite Drive's copies too.
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    daily_path = get_daily_path(today_str)
    await asyncio.to_thread(download_file_from_drive, os.path.basename(daily_path), daily_path)
    await asyncio.to_thread(download_file_from_drive, COMBINED_FILENAME, COMBINED_PATH)

    start_streamer(UPSTOX_ACCESS_TOKEN)

    logger.info("Starting up active 1s real-time loops (timestamps in IST)...")
    await asyncio.gather(
        seconds_timer_loop(),
        google_drive_sync_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script stopped manually by user.")
