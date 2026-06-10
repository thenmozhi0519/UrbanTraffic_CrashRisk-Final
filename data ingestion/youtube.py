import os
import json
import time
import logging
from datetime import datetime
from googleapiclient.discovery import build

# ================= BASE PATH =================

# Get project root directory (used for all file paths)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ================= CONFIG =================

# Load API key from config file (secure way instead of hardcoding)
config_path = os.path.join(BASE_DIR, "config.json")

with open(config_path) as f:
    config = json.load(f)

API_KEY = config["YOUTUBE_API_KEY"]

# Initialize YouTube API client
YOUTUBE = build("youtube", "v3", developerKey=API_KEY)


# Search keywords (used to fetch relevant videos)
QUERIES = [
    "road accidents",
    "traffic crash news",
    "road safety awareness"
]

# Pagination config (handle large data)
# 5 pages × 50 results = 250 videos per query
MAX_RESULTS = 50        # max results per request
TOTAL_PAGES = 5        # number of pages to fetch


# ================= PATH =================

# Bronze layer storage for YouTube data
BRONZE_PATH = os.path.join(BASE_DIR, "data_lake", "bronze", "youtube_sentiment")

# State file for incremental ingestion
STATE_FILE = os.path.join(BASE_DIR, "youtube_last_timestamp.txt")

# Create folder if not exists
os.makedirs(BRONZE_PATH, exist_ok=True)


# ================= LOGGING =================

# Logging helps track ingestion status and errors
logging.basicConfig(
    filename=os.path.join(BASE_DIR, "youtube_ingestion.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# ================= STATE MANAGEMENT =================

def read_last_timestamp():
    """
    Read last processed timestamp.
    Used to fetch only new videos (incremental load).
    """
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return f.read().strip()
    return None


def write_last_timestamp(ts):
    """
    Save latest timestamp after ingestion.
    """
    with open(STATE_FILE, "w") as f:
        f.write(ts)


# ================= FETCH DATA =================

def fetch_data():
    """
    Main ingestion logic:
    - Loop through queries
    - Handle pagination
    - Fetch video metadata
    - Track latest timestamp
    """

    last_ts = read_last_timestamp()
    final_data = []

    # Track latest timestamp in this run
    max_ts = last_ts

    for query in QUERIES:
        next_page_token = None

        for page in range(TOTAL_PAGES):
            try:
                # API request (search videos)
                request = YOUTUBE.search().list(
                    q=query,
                    part="snippet",
                    maxResults=MAX_RESULTS,
                    pageToken=next_page_token,
                    type="video",

                    # Incremental filter → fetch only new videos
                    publishedAfter=last_ts if last_ts else None
                )

                response = request.execute()

            except Exception as e:
                logging.error(f"API error: {e}")
                time.sleep(2)
                continue

            items = response.get("items", [])

            # Stop if no more data
            if not items:
                break

            for item in items:
                try:
                    video_id = item["id"]["videoId"]
                    snippet = item["snippet"]

                    # Extract required fields + add metadata
                    record = {
                        "video_id": video_id,
                        "title": snippet.get("title"),
                        "channel": snippet.get("channelTitle"),
                        "published_at": snippet.get("publishedAt"),
                        "query": query,

                        # Metadata for tracking
                        "ingestion_time": str(datetime.now()),
                        "source": "youtube_api",
                        "batch_id": str(int(time.time()))
                    }

                    final_data.append(record)

                    # Track latest published timestamp
                    pub_time = snippet.get("publishedAt")
                    if pub_time and (not max_ts or pub_time > max_ts):
                        max_ts = pub_time

                except Exception as err:
                    logging.warning(f"Skipping record: {err}")

            # Pagination → move to next page
            next_page_token = response.get("nextPageToken")

            if not next_page_token:
                break

            # Delay to avoid API quota issues
            time.sleep(1)

    return final_data, max_ts


# ================= SAVE DATA =================

def save_data(data):
    """
    Save raw data into Bronze layer (partitioned by date)
    """
    if not data:
        print("No data")
        return

    # Partition folder (date-based)
    today = datetime.now().strftime("%Y-%m-%d")
    folder = os.path.join(BRONZE_PATH, today)
    os.makedirs(folder, exist_ok=True)

    # Unique file per batch
    file_path = os.path.join(folder, f"youtube_{int(time.time())}.json")

    # Store as JSON (raw format)
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logging.info(f"Saved {len(data)} records")


# ================= MAIN =================

if __name__ == "__main__":
    try:
        # Fetch data from API
        data, new_ts = fetch_data()

        # Save to Bronze layer
        save_data(data)

        # Update state for next run
        if new_ts:
            write_last_timestamp(new_ts)

        print("YouTube Bronze Completed")

    except Exception as e:
        logging.error(f"Fatal error: {e}")