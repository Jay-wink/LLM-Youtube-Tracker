import csv
import math
import os
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv
from googleapiclient.discovery import build

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

CHANNELS_CSV = DATA_DIR / "channels.csv"
CANDIDATES_CSV = DATA_DIR / "channel_candidates.csv"

DISCOVERY_QUERIES = [
    "large language model",
    "llm",
    "openai",
    "anthropic",
    "claude ai",
    "gemini ai",
    "ai coding",
    "ai news",
]

MAX_CHANNEL_RESULTS_PER_QUERY = 5
MAX_VIDEO_RESULTS_PER_QUERY = 5
RECENT_VIDEOS_TO_CHECK = 10

PROMOTION_SCORE_HINT = 0.75  # only a hint for later promote step


def load_existing_channels() -> Dict[str, dict]:
    results = {}

    if CHANNELS_CSV.exists():
        with open(CHANNELS_CSV, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                channel_id = row.get("channel_id", "").strip()
                if channel_id:
                    results[channel_id] = row

    return results


def load_existing_candidates() -> Dict[str, dict]:
    results = {}

    if CANDIDATES_CSV.exists():
        with open(CANDIDATES_CSV, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                channel_id = row.get("channel_id", "").strip()
                if channel_id:
                    results[channel_id] = row

    return results


def save_candidates(candidates: List[dict]) -> None:
    fieldnames = [
        "channel_name",
        "channel_id",
        "status",
        "source",
        "discovered_query",
        "subscriber_count",
        "channel_view_count",
        "video_count",
        "recent_llm_ratio",
        "avg_recent_views",
        "best_recent_video_views",
        "score",
        "weeks_below_threshold",
        "last_seen",
    ]

    with open(CANDIDATES_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in candidates:
            writer.writerow(row)


def search_channels(youtube, query: str, order: str) -> List[str]:
    response = youtube.search().list(
        part="snippet",
        q=query,
        type="channel",
        order=order,
        maxResults=MAX_CHANNEL_RESULTS_PER_QUERY,
    ).execute()

    ids = []
    for item in response.get("items", []):
        channel_id = item.get("snippet", {}).get("channelId")
        if channel_id:
            ids.append(channel_id)
    return ids


def search_videos_and_extract_channels(youtube, query: str, published_after: str) -> List[str]:
    response = youtube.search().list(
        part="snippet",
        q=query,
        type="video",
        order="viewCount",
        publishedAfter=published_after,
        maxResults=MAX_VIDEO_RESULTS_PER_QUERY,
    ).execute()

    ids = []
    for item in response.get("items", []):
        channel_id = item.get("snippet", {}).get("channelId")
        if channel_id:
            ids.append(channel_id)
    return ids


def get_channel_details(youtube, channel_ids: List[str]) -> Dict[str, dict]:
    results = {}

    if not channel_ids:
        return results

    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i + 50]
        response = youtube.channels().list(
            part="snippet,statistics,contentDetails",
            id=",".join(batch),
            maxResults=len(batch),
        ).execute()

        for item in response.get("items", []):
            results[item["id"]] = item

    return results


def get_recent_videos_from_uploads(youtube, uploads_playlist_id: str, max_results: int = 10) -> List[dict]:
    try:
        response = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=max_results,
        ).execute()
    except Exception as e:
        return []

    videos = []
    for item in response.get("items", []):
        video_id = item.get("contentDetails", {}).get("videoId")
        snippet = item.get("snippet", {})
        if not video_id:
            continue

        videos.append({
            "video_id": video_id,
            "title": snippet.get("title", ""),
            "description": snippet.get("description", ""),
            "published_at": snippet.get("publishedAt", ""),
        })

    return videos


def hydrate_video_stats(youtube, video_ids: List[str]) -> Dict[str, dict]:
    results = {}

    if not video_ids:
        return results

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        response = youtube.videos().list(
            part="snippet,statistics",
            id=",".join(batch),
            maxResults=len(batch),
        ).execute()

        for item in response.get("items", []):
            results[item["id"]] = item

    return results


def looks_llm_related_text(text: str) -> bool:
    text = text.lower()
    keywords = [
        "llm", "large language model", "openai", "anthropic", "claude",
        "gemini", "gpt", "ai model", "foundation model", "agent",
        "ai coding", "prompt", "fine-tuning", "reasoning model"
    ]
    return any(k in text for k in keywords)


def compute_recent_llm_ratio(videos: List[dict]) -> float:
    if not videos:
        return 0.0

    hits = 0
    for v in videos:
        text = f"{v.get('title', '')} {v.get('description', '')}"
        if looks_llm_related_text(text):
            hits += 1
    return hits / len(videos)


def safe_int(value, default=0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def log_norm(x: float, max_x: float) -> float:
    if x <= 0 or max_x <= 0:
        return 0.0
    return min(math.log1p(x) / math.log1p(max_x), 1.0)


def compute_channel_relevance(channel_title: str, channel_description: str) -> float:
    text = f"{channel_title} {channel_description}".lower()

    strong = ["llm", "openai", "anthropic", "claude", "gemini", "large language model"]
    medium = ["ai", "agent", "prompt", "model", "machine learning"]

    score = 0.0
    for k in strong:
        if k in text:
            score += 0.2
    for k in medium:
        if k in text:
            score += 0.08

    return min(score, 1.0)


def score_candidate(channel_item: dict, recent_videos: List[dict], recent_video_stats: Dict[str, dict]) -> dict:
    snippet = channel_item.get("snippet", {})
    statistics = channel_item.get("statistics", {})

    channel_name = snippet.get("title", "")
    channel_description = snippet.get("description", "")
    subscriber_count = safe_int(statistics.get("subscriberCount", 0))
    channel_view_count = safe_int(statistics.get("viewCount", 0))
    video_count = safe_int(statistics.get("videoCount", 0))

    recent_llm_ratio = compute_recent_llm_ratio(recent_videos)

    view_counts = []
    best_recent_video_views = 0

    for v in recent_videos:
        video_id = v["video_id"]
        stats_item = recent_video_stats.get(video_id, {})
        stats = stats_item.get("statistics", {})
        views = safe_int(stats.get("viewCount", 0))
        view_counts.append(views)
        best_recent_video_views = max(best_recent_video_views, views)

    avg_recent_views = int(sum(view_counts) / len(view_counts)) if view_counts else 0

    channel_relevance = compute_channel_relevance(channel_name, channel_description)
    subscriber_norm = log_norm(subscriber_count, 10_000_000)
    avg_views_norm = log_norm(avg_recent_views, 1_000_000)

    score = (
        0.35 * channel_relevance
        + 0.30 * recent_llm_ratio
        + 0.20 * avg_views_norm
        + 0.15 * subscriber_norm
    )

    return {
        "subscriber_count": subscriber_count,
        "channel_view_count": channel_view_count,
        "video_count": video_count,
        "recent_llm_ratio": round(recent_llm_ratio, 3),
        "avg_recent_views": avg_recent_views,
        "best_recent_video_views": best_recent_video_views,
        "score": round(score, 3),
    }


def main():
    load_dotenv()
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise ValueError("Missing YOUTUBE_API_KEY in .env")

    youtube = build("youtube", "v3", developerKey=api_key)

    existing_channels = load_existing_channels()
    existing_candidates = load_existing_candidates()

    discovered_queries_by_channel: Dict[str, set] = {}
    discovered_channel_ids = set()

    # recent 30 days for video discovery
    published_after = (datetime.now(UTC) - timedelta(days=30)).isoformat().replace("+00:00", "Z")

    print("Discovering candidate channels...")

    for query in DISCOVERY_QUERIES:
        # route A: search channels directly
        for order in ["relevance", "viewCount"]:
            try:
                ids = search_channels(youtube, query, order)
                for cid in ids:
                    discovered_channel_ids.add(cid)
                    discovered_queries_by_channel.setdefault(cid, set()).add(query)
            except Exception as e:
                print(f"Channel search failed for query='{query}', order='{order}': {e}")

        # route B: search videos, then infer channels
        try:
            ids = search_videos_and_extract_channels(youtube, query, published_after)
            for cid in ids:
                discovered_channel_ids.add(cid)
                discovered_queries_by_channel.setdefault(cid, set()).add(query)
        except Exception as e:
            print(f"Video search failed for query='{query}': {e}")

    # don't rediscover channels already tracked unless you want candidate history too
    channel_details = get_channel_details(youtube, list(discovered_channel_ids))

    updated_candidates = {}

    for channel_id, channel_item in channel_details.items():
        snippet = channel_item.get("snippet", {})
        content_details = channel_item.get("contentDetails", {})
        related_playlists = content_details.get("relatedPlaylists", {})
        uploads_playlist_id = related_playlists.get("uploads")

        if not uploads_playlist_id:
            continue

        channel_name = snippet.get("title", "")
        recent_videos = get_recent_videos_from_uploads(
            youtube,
            uploads_playlist_id,
            max_results=RECENT_VIDEOS_TO_CHECK,
        )

        if not recent_videos:
            print(f"Skipping channel {channel_name} ({channel_id}) because no usable recent videos were found.")
            continue

        recent_video_ids = [v["video_id"] for v in recent_videos]
        recent_video_stats = hydrate_video_stats(youtube, recent_video_ids)

        metrics = score_candidate(channel_item, recent_videos, recent_video_stats)

        discovered_query = "|".join(sorted(discovered_queries_by_channel.get(channel_id, set())))
        today = datetime.now(UTC).date().isoformat()

        already_tracked = channel_id in existing_channels
        old_row = existing_candidates.get(channel_id, {})

        previous_weeks_below = safe_int(old_row.get("weeks_below_threshold", 0))
        score = metrics["score"]

        if already_tracked:
            status = "promoted"
            weeks_below_threshold = 0
        else:
            if score >= PROMOTION_SCORE_HINT:
                status = old_row.get("status", "candidate")
                weeks_below_threshold = 0
            else:
                status = old_row.get("status", "candidate")
                weeks_below_threshold = previous_weeks_below + 1

                if weeks_below_threshold >= 4:
                    status = "discarded"

        updated_candidates[channel_id] = {
            "channel_name": channel_name,
            "channel_id": channel_id,
            "status": status,
            "source": "auto",
            "discovered_query": discovered_query,
            "subscriber_count": metrics["subscriber_count"],
            "channel_view_count": metrics["channel_view_count"],
            "video_count": metrics["video_count"],
            "recent_llm_ratio": metrics["recent_llm_ratio"],
            "avg_recent_views": metrics["avg_recent_views"],
            "best_recent_video_views": metrics["best_recent_video_views"],
            "score": metrics["score"],
            "weeks_below_threshold": weeks_below_threshold,
            "last_seen": today,
        }

    # keep older candidates if they were not rediscovered this week
    # but preserve them in the observation pool
    for channel_id, old_row in existing_candidates.items():
        if channel_id not in updated_candidates:
            updated_candidates[channel_id] = old_row

    # sort by status then score desc
    def sort_key(row: dict):
        status_order = {
            "candidate": 0,
            "promoted": 1,
            "discarded": 2,
        }
        return (
            status_order.get(row.get("status", "candidate"), 99),
            -float(row.get("score", 0) or 0),
            row.get("channel_name", ""),
        )

    final_rows = sorted(updated_candidates.values(), key=sort_key)

    save_candidates(final_rows)

    print(f"Saved {len(final_rows)} rows to {CANDIDATES_CSV}")


if __name__ == "__main__":
    main()