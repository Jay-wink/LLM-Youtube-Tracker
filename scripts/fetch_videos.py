import csv
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from googleapiclient.discovery import build

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
CHANNELS_CSV = DATA_DIR / "channels.csv"
OUTPUT_JSON = DATA_DIR / "videos_raw.json"

# TODO: follow popular channels that pop up on the "lit" list
def load_channels():
    channels = []
    with open(CHANNELS_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["channel_id"].strip():
                channels.append(row)
    return channels


def get_uploads_playlist_id(youtube, channel_id):
    response = youtube.channels().list(
        part="contentDetails,snippet",
        id=channel_id
    ).execute()

    items = response.get("items", [])
    if not items:
        return None, None

    uploads_playlist = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
    channel_title = items[0]["snippet"]["title"]
    return uploads_playlist, channel_title


def get_recent_videos_from_playlist(youtube, playlist_id, max_results=5):
    response = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=max_results
    ).execute()

    videos = []
    for item in response.get("items", []):
        snippet = item["snippet"]
        resource_id = snippet.get("resourceId", {})
        video_id = resource_id.get("videoId")

        if not video_id:
            continue

        videos.append({
            "video_id": video_id,
            "title": snippet.get("title", ""),
            "description": snippet.get("description", ""),
            "published_at": snippet.get("publishedAt", ""),
            "thumbnail": snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
        })
    return videos


def main():
    load_dotenv()
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise ValueError("Missing YOUTUBE_API_KEY in .env")

    youtube = build("youtube", "v3", developerKey=api_key)

    channels = load_channels()
    all_videos = []

    for channel in channels:
        channel_id = channel["channel_id"]
        uploads_playlist_id, actual_channel_name = get_uploads_playlist_id(youtube, channel_id)

        if not uploads_playlist_id:
            print(f"Could not find uploads playlist for {channel_id}")
            continue

        videos = get_recent_videos_from_playlist(youtube, uploads_playlist_id, max_results=5)

        for video in videos:
            video["channel_id"] = channel_id
            video["channel_name"] = actual_channel_name or channel["channel_name"]
            video["notes"] = channel.get("notes", "")
            video["url"] = f"https://www.youtube.com/watch?v={video['video_id']}"

        all_videos.extend(videos)
        print(f"Fetched {len(videos)} videos from {actual_channel_name}")

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_videos, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(all_videos)} videos to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()