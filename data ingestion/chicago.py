import requests
import json
import os
import time
import logging
from datetime import datetime

# ================= CONFIG =================

# Chicago Open Data API (Crash dataset)
API_ENDPOINT = "https://data.cityofchicago.org/resource/85ca-t3if.json"

# Number of records per API call (batch processing)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))

# Delay between API calls (avoid rate limit)
SLEEP_TIME = 1

# Retry count for API failures
MAX_RETRIES = 3


# ================= PATH SETUP =================

# Get current file directory
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Bronze layer storage (raw data lake)
DATA_PATH = os.path.join(ROOT_DIR, "../data_lake/bronze/crashes_partitioned/")

# State file to track last processed timestamp (incremental load)
STATE_FILE = os.path.join(ROOT_DIR, "last_timestamp.txt")

# Create folder if not exists
os.makedirs(DATA_PATH, exist_ok=True)


# ================= LOGGING =================

# Logging helps track success, failure, retries
logging.basicConfig(
    filename=os.path.join(ROOT_DIR, "chicago_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# ================= STATE MANAGEMENT =================

def read_last_timestamp():
    """
    Read last processed timestamp from file.
    Used for incremental ingestion (only new data).
    """
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return f.read().strip()
    return None


def write_last_timestamp(ts):
    """
    Save latest timestamp after processing.
    """
    with open(STATE_FILE, "w") as f:
        f.write(ts)


# ================= BUILD API URL =================

def build_url(last_ts):
    """
    Build API query dynamically.
    - Limit records (batch)
    - Order by crash_date
    - Apply filter for incremental load
    """
    query = f"$limit={BATCH_SIZE}&$order=crash_date"

    # Fetch only new records (incremental ingestion)
    if last_ts:
        query += f"&$where=crash_date > '{last_ts}'"

    return f"{API_ENDPOINT}?{query}"


# ================= FETCH DATA =================

def fetch_data():
    """
    Main ingestion function:
    - Calls API
    - Handles retries
    - Cleans data
    - Stores in Bronze layer
    - Updates state
    """

    # Get last processed timestamp
    last_ts = read_last_timestamp()
    print("Last timestamp:", last_ts)

    while True:
        url = build_url(last_ts)
        print("Fetching:", url)

        records = None

        # 🔁 Retry logic (handles API failures)
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, timeout=20)
                response.raise_for_status()
                records = response.json()
                break
            except Exception as e:
                logging.warning(f"Retry {attempt+1} failed: {e}")
                time.sleep(2)

        # If all retries failed → stop job
        if records is None:
            logging.error("All retries failed")
            return False

        # No new data → stop loop
        if not records:
            print("No new data")
            break

        # ================= CLEAN DATA =================

        cleaned_records = []

        for rec in records:
            try:
                # Add metadata for tracking
                rec["ingestion_time"] = str(datetime.now())
                rec["source"] = "chicago_api"
                rec["batch_id"] = str(int(time.time()))

                # Ensure required field exists
                if "crash_date" not in rec:
                    continue

                cleaned_records.append(rec)

            except Exception as err:
                logging.warning(f"Skipping bad record: {err}")

        # ================= SAVE TO BRONZE =================

        # Partition data by date (good for scalability)
        today = datetime.now().strftime("%Y-%m-%d")
        folder = os.path.join(DATA_PATH, today)
        os.makedirs(folder, exist_ok=True)

        # Unique file name per batch
        file_name = f"crash_{int(time.time())}.json"
        file_path = os.path.join(folder, file_name)

        # Store as JSON (raw Bronze layer)
        with open(file_path, "w") as f:
            json.dump(cleaned_records, f, indent=2)

        print(f"Saved {len(cleaned_records)} records")
        logging.info(f"Saved {len(cleaned_records)} records")

        # ================= UPDATE STATE =================

        # Get latest timestamp for next run
        last_ts = cleaned_records[-1].get("crash_date", last_ts)
        write_last_timestamp(last_ts)

        # Sleep to avoid API throttling
        time.sleep(SLEEP_TIME)

    return True


# ================= MAIN =================

if __name__ == "__main__":
    print("Starting Chicago Bronze Ingestion")

    try:
        status = fetch_data()

        if status:
            print("Completed Successfully")
            logging.info("Job completed")
        else:
            print("Job Failed")
            logging.error("Job failed")

    except Exception as e:
        print("Fatal Error:", e)
        logging.error(f"Fatal error: {e}")