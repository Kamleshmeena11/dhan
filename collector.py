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

# Global memory array to store streaming raw ticks inside the 1-second interval window
tick_store = []

# Tracks the timestamp of the last bar actually written, so we can detect
# any skipped seconds (event loop stalls, reconnects, etc.) between bars.
last_bar_time = None


def process_ticks_to_1s(bar_time: datetime):
    """Compiles all raw stream ticks collected over the past second into an OHLCV bar.

    bar_time is the exact second boundary this bar belongs to (computed by the
    timer loop BEFORE any processing happens), not the wall-clock time at the
    moment this function actually runs. That's what removes the ~1s lag.
    """
    global tick_store, last_bar_time

    bar_str = bar_time.strftime("%Y-%m-%d %H:%M:%S")

    # --- GAP DEBUG 1: did we skip whole second(s) since the last bar? ---
    # This catches event-loop stalls, WebSocket reconnects, or the script
    # hanging -- anything that stops seconds_timer_loop from firing on time.
    if last_bar_time is not None:
        gap = round((bar_time - last_bar_time).total_seconds())
        if gap > 1:
            missing_seconds = gap - 1
            logger.warning(
                f"DATA GAP: {missing_seconds} whole second(s) missing between "
                f"{last_bar_time.strftime('%Y-%m-%d %H:%M:%S')} and {bar_str} IST "
                f"(no bars written for that span, no fabricated/forward-filled data)"
            )
    last_bar_time = bar_time

    # --- GAP DEBUG 2: did the broker send us zero ticks this second? ---
    # Confirmed cause (per Fyers collector history) is broker-side delivery
    # failure, not the index actually going silent. Log it plainly instead of
    # silently skipping -- an empty bar is still information.
    if not tick_store:
        logger.warning(f"NO TICKS received for bar {bar_str} IST - broker delivery gap, bar skipped")
        return

    tick_count = len(tick_store)
    prices = [t["price"] for t in tick_store]
    volumes = [t.get("volume", 0) for t in tick_store]

    new_row = {
        "timestamp": bar_str,
        "instrument_key": INSTRUMENT_KEY,
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "volume": sum(volumes)
    }

    # Wipe queue clean for the next second's window immediately
    tick_store = []

    # Write data row straight to the destination CSV
    df = pd.DataFrame([new_row])
    file_exists = os.path.isfile(CSV_FILENAME)
    df.to_csv(CSV_FILENAME, mode="a", index=False, header=not file_exists)
    logger.info(f"Saved 1s Bar -> {bar_str} IST | Close: {new_row['close']} | ticks: {tick_count}")


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
    """Triggers the OHLC computation loop precisely at the turn of every clock second.

    Key fix: the boundary time is computed HERE, right before sleeping to it,
    and handed to process_ticks_to_1s(). The old code called datetime.now()
    a second time *inside* process_ticks_to_1s(), after the sleep + any event
    loop scheduling jitter had already elapsed -- which is exactly what made
    every printed bar look ~1s (or more, under load) behind the real tick.
    """
    while True:
        now = time.time()
        sleep_time = 1.0 - (now % 1.0)
        boundary_epoch = now + sleep_time  # the exact second boundary we're waiting for
        await asyncio.sleep(sleep_time)

        # Ticks just flushed were collected during [boundary_epoch - 1, boundary_epoch),
        # i.e. the second that just ELAPSED, not the second we just landed on.
        # Label the bar with the start of that window so it matches Upstox's own
        # per-tick timestamps (10:05:03 ticks -> bar "10:05:03", not "10:05:04").
        bar_time = datetime.fromtimestamp(boundary_epoch - 1.0, tz=IST)
        process_ticks_to_1s(bar_time)


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
    global tick_store
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
            tick_store.append({
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
