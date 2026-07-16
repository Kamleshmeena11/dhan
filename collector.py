import os
import sys
import time
import json
import logging
import asyncio
from datetime import datetime
import pandas as pd

# Dhan v2.2.0 imports
from dhanhq import DhanContext, MarketFeed
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
CLIENT_ID = os.environ.get("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN")

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")

INSTRUMENTS = [
    (MarketFeed.IDX, "13", MarketFeed.Ticker)  # IDX (0) = NSE Indices segment ("IDX_I"), ID "13" = Nifty 50 Index
]

tick_store = {}
CSV_FILENAME = "candles_1s_all.csv"


def upload_to_drive():
    if not os.path.exists(CSV_FILENAME):
        logger.info(f"Local file {CSV_FILENAME} does not exist yet. Skipping sync...")
        return
    if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
        logger.warning("Google Drive credentials missing. Skipping cloud backup.")
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
            logger.info(f"Updating existing Google Drive file: {file_id}")
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            logger.info("Creating new file on Google Drive...")
            file_metadata = {"name": CSV_FILENAME}
            new_file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
            logger.info(f"File created successfully with ID: {new_file.get('id')}")
    except Exception as e:
        logger.error(f"Error syncing to Google Drive: {e}")


def process_ticks_to_1s():
    now = datetime.now()
    timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

    new_rows = []
    for inst_id, ticks in list(tick_store.items()):
        if not ticks:
            continue
        prices = [t["price"] for t in ticks]
        volumes = [t.get("volume", 0) for t in ticks]

        new_rows.append({
            "timestamp": timestamp_str,
            "instrument_id": inst_id,
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": sum(volumes)
        })
        tick_store[inst_id] = []

    if new_rows:
        df = pd.DataFrame(new_rows)
        file_exists = os.path.isfile(CSV_FILENAME)
        df.to_csv(CSV_FILENAME, mode="a", index=False, header=not file_exists)
        logger.info(f"Saved {len(new_rows)} rows for timestamp {timestamp_str}")


def on_connect(instance):
    logger.info("Successfully connected to Dhan Market Feed WebSockets.")


def on_message(instance, message):
    """Processes a single parsed tick dict returned by MarketFeed.get_instrument_data().

    Ticker-mode packets look like:
        {"type": "Ticker Data", "exchange_segment": ..., "security_id": ...,
         "LTP": "24123.45", "LTT": ...}
    Note: key is "LTP" (a string), not "last_traded_price"/"price", and there is
    no "volume" field in Ticker mode (only in Quote/Full mode).
    """
    try:
        if not message or message.get("type") != "Ticker Data":
            return

        inst_id = message.get("security_id")
        ltp = message.get("LTP")
        if inst_id is None or ltp is None:
            return

        inst_id = str(inst_id)
        tick_store.setdefault(inst_id, []).append({
            "price": float(ltp),
            "volume": 0.0  # not available in Ticker mode
        })
    except Exception as e:
        logger.error(f"Error handling live feed message: {e}")


async def feed_receive_loop(feed):
    """Owns the actual receive loop. feed.connect() alone only opens the socket
    and subscribes - it never reads messages. get_instrument_data() must be
    awaited repeatedly to pull data off the wire."""
    await feed.connect()
    while True:
        try:
            data = await feed.get_instrument_data()
            on_message(feed, data)
        except Exception as e:
            logger.error(f"Feed receive error: {e}. Reconnecting in 3s...")
            await asyncio.sleep(3)
            try:
                await feed.connect()
            except Exception as reconnect_err:
                logger.error(f"Reconnect failed: {reconnect_err}")


async def seconds_timer_loop():
    while True:
        now = time.time()
        sleep_time = 1.0 - (now % 1.0)
        await asyncio.sleep(sleep_time)
        process_ticks_to_1s()


async def google_drive_sync_loop():
    while True:
        await asyncio.sleep(10)
        upload_to_drive()


async def main():
    if not CLIENT_ID or not ACCESS_TOKEN:
        logger.error("Dhan credentials (DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN) are missing!")
        sys.exit(1)

    dhan_context = DhanContext(client_id=CLIENT_ID, access_token=ACCESS_TOKEN)
    feed = MarketFeed(
        dhan_context=dhan_context,
        instruments=INSTRUMENTS,
        version="v2"
    )
    feed.on_connect = on_connect
    # NOTE: feed.on_message is intentionally NOT wired here - the SDK only
    # invokes it from inside its own run()/_run_async(), which we're not
    # using so we can share the event loop with the timer/sync tasks below.
    # feed_receive_loop() calls on_message() manually instead.

    tasks = [
        asyncio.create_task(feed_receive_loop(feed)),
        asyncio.create_task(seconds_timer_loop()),
        asyncio.create_task(google_drive_sync_loop())
    ]

    logger.info("Starting loops and WebSocket client connection...")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script stopped manually by user.")
