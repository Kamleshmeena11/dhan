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

# --- Timezone ---
IST = ZoneInfo("Asia/Kolkata")

# --- Configuration & Credentials ---
UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")
INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"
CSV_FILENAME = "candles_1s_upstox.csv"

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


def write_bar(bar_time: datetime, ticks: list):
    """Writes one finalized 1s OHLCV bar built entirely from ticks whose own
    exchange timestamp (ltt) falls in this second."""
    bar_str = bar_time.strftime("%Y-%m-%d %H:%M:%S")
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

    df = pd.DataFrame([new_row])
    file_exists = os.path.isfile(CSV_FILENAME)
    df.to_csv(CSV_FILENAME, mode="a", index=False, header=not file_exists)
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
        # upload_to_drive() is a blocking synchronous HTTP call. Running it
        # directly on the event loop freezes seconds_timer_loop for however
        # long the request takes (~1-3s here), which is exactly what caused
        # entire 1s bars to go missing right after every sync. to_thread()
        # runs it on a worker thread so the per-second timer keeps firing.
        await asyncio.to_thread(upload_to_drive)


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
