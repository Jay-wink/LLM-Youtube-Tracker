import json
import time
from pathlib import Path

from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
)

ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = ROOT / "data" / "videos_raw.json"
OUTPUT_FILE = ROOT / "data" / "videos_with_transcripts.json"


def log(msg: str) -> None:
    print(msg, flush=True)


def join_fetched_transcript(fetched_transcript) -> str:
    return " ".join(snippet.text for snippet in fetched_transcript).strip()


def load_existing_transcripts() -> dict:
    """
    Returns:
        dict[video_id] -> existing transcript record
    """
    if not OUTPUT_FILE.exists():
        return {}

    raw_text = OUTPUT_FILE.read_text(encoding="utf-8").strip()
    if not raw_text:
        return {}

    try:
        data = json.loads(raw_text)
    except Exception:
        return {}

    existing = {}
    for item in data:
        video_id = item.get("video_id", "").strip()
        if video_id:
            existing[video_id] = item

    return existing


def should_reuse_existing(existing_item: dict) -> bool:
    """
    Reuse anything we've already attempted.
    This prevents hitting the same video every day.
    """
    if not existing_item:
        return False

    source = (existing_item.get("transcript_source") or "").strip()

    # reuse successful results
    if source in {"manual_caption", "auto_caption"} or source.startswith("caption:"):
        return True

    # reuse known unsuccessful states too
    if source in {"missing", "request_blocked"}:
        return True

    if source.startswith("error:"):
        return True

    return False


def fetch_transcript_for_video(ytt_api: YouTubeTranscriptApi, video_id: str) -> tuple[str, str]:
    """
    Returns:
        (transcript_text, transcript_source)
    """
    # 1) Try direct fetch first
    try:
        fetched = ytt_api.fetch(video_id, languages=["en", "en-US", "en-GB"])
        text = join_fetched_transcript(fetched)
        if text:
            source = "auto_caption" if fetched.is_generated else "manual_caption"
            return text, source
    except (NoTranscriptFound, TranscriptsDisabled, VideoUnavailable):
        pass
    except Exception as e:
        error_text = str(e)
        if "IpBlocked" in error_text or "RequestBlocked" in error_text:
            return "", "request_blocked"
        direct_fetch_error = f"{type(e).__name__}: {e}"
    else:
        direct_fetch_error = ""

    # 2) Try listing available transcripts
    try:
        transcript_list = ytt_api.list(video_id)
    except (NoTranscriptFound, TranscriptsDisabled, VideoUnavailable):
        return "", "missing"
    except Exception as e:
        error_text = str(e)
        if "IpBlocked" in error_text or "RequestBlocked" in error_text:
            return "", "request_blocked"
        return "", f"error:list:{type(e).__name__}"

    # 3) Manual English
    try:
        transcript = transcript_list.find_manually_created_transcript(["en", "en-US", "en-GB"])
        fetched = transcript.fetch()
        text = join_fetched_transcript(fetched)
        if text:
            return text, "manual_caption"
    except Exception as e:
        error_text = str(e)
        if "IpBlocked" in error_text or "RequestBlocked" in error_text:
            return "", "request_blocked"
        manual_error = f"{type(e).__name__}: {e}"
    else:
        manual_error = ""

    # 4) Auto English
    try:
        transcript = transcript_list.find_generated_transcript(["en", "en-US", "en-GB"])
        fetched = transcript.fetch()
        text = join_fetched_transcript(fetched)
        if text:
            return text, "auto_caption"
    except Exception as e:
        error_text = str(e)
        if "IpBlocked" in error_text or "RequestBlocked" in error_text:
            return "", "request_blocked"
        generated_error = f"{type(e).__name__}: {e}"
    else:
        generated_error = ""

    # 5) Any available transcript
    any_errors = []
    found_any = False
    for transcript in transcript_list:
        found_any = True
        try:
            fetched = transcript.fetch()
            text = join_fetched_transcript(fetched)
            if text:
                return text, f"caption:{transcript.language_code}"
        except Exception as e:
            error_text = str(e)
            if "IpBlocked" in error_text or "RequestBlocked" in error_text:
                return "", "request_blocked"
            any_errors.append(f"{transcript.language_code}:{type(e).__name__}")

    if not found_any:
        return "", "missing"

    debug_parts = []
    if "direct_fetch_error" in locals() and direct_fetch_error:
        debug_parts.append(f"direct={direct_fetch_error}")
    if manual_error:
        debug_parts.append(f"manual={manual_error}")
    if generated_error:
        debug_parts.append(f"generated={generated_error}")
    if any_errors:
        debug_parts.append(f"any={','.join(any_errors[:3])}")

    if debug_parts:
        return "", "error:" + " | ".join(debug_parts)

    return "", "missing"


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_FILE}")

    raw_text = INPUT_FILE.read_text(encoding="utf-8").strip()
    if not raw_text:
        raise ValueError(f"Input file is empty: {INPUT_FILE}")

    videos = json.loads(raw_text)
    existing_map = load_existing_transcripts()
    ytt_api = YouTubeTranscriptApi()

    log(f"[Main] Loaded {len(videos)} videos from {INPUT_FILE}")
    log(f"[Main] Loaded {len(existing_map)} existing transcript records from cache")

    output = []
    fetched_count = 0
    reused_count = 0

    for i, video in enumerate(videos, start=1):
        video_id = video.get("video_id", "").strip()
        title = video.get("title", "")

        existing_item = existing_map.get(video_id)

        if existing_item and should_reuse_existing(existing_item):
            reused_item = dict(video)
            reused_item["transcript"] = existing_item.get("transcript", "")
            reused_item["transcript_source"] = existing_item.get("transcript_source", "")
            output.append(reused_item)

            reused_count += 1
            log(f"[{i}/{len(videos)}] Reusing cached transcript result: {title} -> {reused_item['transcript_source']}")
            continue

        log(f"[{i}/{len(videos)}] Fetching transcript: {title}")

        transcript_text, transcript_source = fetch_transcript_for_video(ytt_api, video_id)

        item = dict(video)
        item["transcript"] = transcript_text
        item["transcript_source"] = transcript_source
        output.append(item)

        fetched_count += 1
        log(f"    -> {transcript_source}")

        # Slow down a little to reduce blocking risk
        time.sleep(2)

    OUTPUT_FILE.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    log(f"[Main] Saved transcripts to: {OUTPUT_FILE}")
    log(f"[Main] Reused cached items: {reused_count}")
    log(f"[Main] Newly fetched items: {fetched_count}")


if __name__ == "__main__":
    main()