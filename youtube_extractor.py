"""
This script is part of a real-time data pipeline project.
It extracts channel and video statistics from the YouTube Data API.
The data is structured and streamed to a Kafka topic in JSON format.

The script avoids duplicate processing by maintaining a record of seen video IDs.
It supports both an initial batch load and continuous updates at fixed intervals.
This design enables scalable and automated ingestion of video data from multiple channels.
"""

# Importing Required Libraries 

import os
import json
import time
import requests
from kafka import KafkaProducer

# Youtube API Key from google developer console 
API_KEY = "AIzaSyDDA-wefZOdpyQ2sEmpuiM7Ik1uoNDFyrQ"

CHANNEL_IDS = {
    "Saregama Tamil": "UCzee67JnEcuvjErRyWP3GpQ",
    "Trend Music": "UCUgP5pdToi19fWe19NVliFg",
    "Sony Music South": "UCn4rEMqKtwBQ6-oEwbd4PcA",
    "Tips Official": "UCJrDMFOdv1I2k8n9oK_V21w",
    "Ilaiyaraajaofficial": "UCVlWr_LN9y80smEMr0KTBOA",
    "Yash Raj Films (YRF)": "UCbTLwN10NoCU4WDzLf1JMOA",
    "T-series Tamil": "UCAEv0ANkT221wXsTnxFnBsQ",
    "Sun Music": "UCLpTl1OxHZnEOJj8gjAycdw",
    "T-Series": "UCq-Fj5jknLsUf-MWSy4_brA",
    "Zee Music Company": "UCFFbwnve3yF62-tVXkTyHqg"
}

BASE_URL = "https://www.googleapis.com/youtube/v3"
SEEN_FILE = "seen_video_ids.json"
KAFKA_TOPIC = "youtube_data"
KAFKA_BROKER = "localhost:9092"

# Create and Initial Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

seen_video_ids = set()

def load_seen_ids():
    """
    Loads previously seen video IDs from a local JSON file.
    This helps avoid reprocessing or duplicate data entries.
    The video IDs are stored in a Python set for fast lookup.
    """
    global seen_video_ids
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE, "r") as f:
            seen_video_ids = set(json.load(f))

def save_seen_ids():
    """
    Saves the set of processed video IDs to disk in JSON format.
    Used to persist duplicate tracking across script executions.
    Ensures only new videos are processed in future runs.
    """
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen_video_ids), f)

def get_channel_info(channel_id):
    """
    Fetches metadata and statistics for a YouTube channel.
    Uses the YouTube Data API to retrieve the upload playlist ID and subscriber stats.
    This is required to access and analyze videos uploaded to the channel.
    """
    url = f"{BASE_URL}/channels?part=snippet,statistics,contentDetails&id={channel_id}&key={API_KEY}"
    try:
        res = requests.get(url).json()
        if "items" not in res or not res["items"]:
            print(f"[ERROR] No data for channel: {channel_id}")
            return None
        item = res["items"][0]
        stats = item["statistics"]
        return {
            "channel_id": channel_id,
            "channel_name": item["snippet"]["title"],
            "subscriber_count": int(stats.get("subscriberCount", 0)),
            "views_count": int(stats.get("viewCount", 0)),
            "total_videos": int(stats.get("videoCount", 0)),
            "playlist_id": item["contentDetails"]["relatedPlaylists"]["uploads"]
        }
    except Exception as e:
        print(f"[ERROR] Channel fetch failed: {e}")
        return None

def get_top_comment(video_id):
    url = f"{BASE_URL}/commentThreads?part=snippet&videoId={video_id}&maxResults=1&key={API_KEY}"
    try:
        res = requests.get(url).json()
        items = res.get("items", [])
        if not items:
            return ""
        return items[0]["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
    except:
        return ""

def fetch_video_stats(playlist_id, channel_name):
    """
    Retrieves the most recent 50 videos from a channel's uploads playlist.
    For each unseen video, it forwards the ID to be fetched and published to Kafka.
    This function acts as the entry point for processing new videos.
    """
    url = f"{BASE_URL}/playlistItems?part=snippet&playlistId={playlist_id}&maxResults=50&key={API_KEY}"
    try:
        res = requests.get(url).json()
        if "items" not in res:
            print(f"[WARN] No videos found for playlist: {playlist_id}")
            return
        for item in res["items"]:
            video_id = item["snippet"]["resourceId"]["videoId"]
            if video_id not in seen_video_ids:
                push_video_by_id(video_id, channel_name, playlist_id)
    except Exception as e:
        print(f"[ERROR] Fetching playlist items failed: {e}")

def push_video_by_id(video_id, channel_name, playlist_id):
    """
    Fetches full statistics and metadata for a specific video using the YouTube API.
    Sends the structured data as a JSON message to a Kafka topic for further processing.
    Also records the video ID to prevent duplicate processing.
    """
    stats_url = f"{BASE_URL}/videos?part=snippet,statistics,contentDetails&id={video_id}&key={API_KEY}"
    try:
        stats_res = requests.get(stats_url).json()
        if "items" not in stats_res:
            return
        video = stats_res["items"][0]
        snippet = video["snippet"]
        statistics = video["statistics"]
        content_details = video["contentDetails"]

        video_data = {
            "playlist_id": playlist_id,
            "channel_name": channel_name,
            "video_id": video_id,
            "title": snippet.get("title", ""),
            "description": snippet.get("description", ""),
            "tags": snippet.get("tags", []),
            "publish_at": snippet.get("publishedAt", ""),
            "views_count": int(statistics.get("viewCount", 0)),
            "like_count": int(statistics.get("likeCount", 0)),
            "comment_count": int(statistics.get("commentCount", 0)),
            "video_duration": content_details.get("duration", "N/A"),
            "comment": get_top_comment(video_id),
            "caption": content_details.get("caption", "false"),
            "definition": content_details.get("definition", "sd")
        }

        producer.send(KAFKA_TOPIC, {"type": "video", "data": video_data})
        seen_video_ids.add(video_id)
        print(f"Pushed video: {video_data['title']}")
    except Exception as e:
        print(f"[ERROR] Video push failed for {video_id}: {e}")

def run_initial_load():
    """
    Performs a one-time full extraction and ingestion of all channel data.
    Pushes channel-level metadata and recent video data to Kafka.
    Suitable for the first run or full refresh scenarios.
    """
    print("Initial batch loading...")
    for name, channel_id in CHANNEL_IDS.items():
        channel_data = get_channel_info(channel_id)
        if channel_data:
            try:
                producer.send(KAFKA_TOPIC, {"type": "channel", "data": channel_data})
                print(f" Pushed channel: {channel_data['channel_name']}")
            except Exception as e:
                print(f"[ERROR] Channel push failed: {e}")
            fetch_video_stats(channel_data["playlist_id"], name)
    save_seen_ids()

def run_streaming_check(interval=600):
    """
    Runs the video extraction and ingestion process at a regular interval.
    Continuously checks for new videos and channel updates.
    Ensures near real-time data collection from YouTube channels.
    """
    while True:
        print("Checking for new videos and stats...")
        for name, channel_id in CHANNEL_IDS.items():
            channel_data = get_channel_info(channel_id)
            if channel_data:
                try:
                    producer.send(KAFKA_TOPIC, {"type": "channel", "data": channel_data})
                    print(f"Updated channel stats: {channel_data['channel_name']}")
                except Exception as e:
                    print(f"[ERROR] Channel update failed: {e}")
                fetch_video_stats(channel_data["playlist_id"], name)
        save_seen_ids()
        print(f"Sleeping for {interval // 60} minutes...\n")
        time.sleep(interval)

if __name__ == "__main__":
    load_seen_ids()
    run_initial_load()
    run_streaming_check(600)


