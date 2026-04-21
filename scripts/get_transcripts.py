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


def join_fetched_transcript(fetched_transcript) -> str:
    # 新版 fetch() 返回 FetchedTranscript，可迭代，元素是对象
    return " ".join(snippet.text for snippet in fetched_transcript).strip()


def fetch_transcript_for_video(video_id: str) -> tuple[str, str]:
    """
    Returns:
        (transcript_text, transcript_source)
    """
    ytt_api = YouTubeTranscriptApi()

    try:
        # manual first, then automatic
        try:
            fetched = ytt_api.fetch(video_id, languages=["en", "en-US", "en-GB"])
            text = join_fetched_transcript(fetched)
            if text:
                source = "auto_caption" if getattr(fetched, "is_generated", False) else "manual_caption"
                return text, source
        except Exception:
            pass

        transcript_list = ytt_api.list(video_id)

        # 1) manual English transcripts
        try:
            transcript = transcript_list.find_manually_created_transcript(["en", "en-US", "en-GB"])
            fetched = transcript.fetch()
            text = join_fetched_transcript(fetched)
            if text:
                return text, "manual_caption"
        except Exception:
            pass

        # 2) automatic English transcripts
        try:
            transcript = transcript_list.find_generated_transcript(["en", "en-US", "en-GB"])
            fetched = transcript.fetch()
            text = join_fetched_transcript(fetched)
            if text:
                return text, "auto_caption"
        except Exception:
            pass

        # 3) any available transcripts
        for transcript in transcript_list:
            try:
                fetched = transcript.fetch()
                text = join_fetched_transcript(fetched)
                if text:
                    return text, f"caption:{transcript.language_code}"
            except Exception:
                continue

        return "", "missing"

    except (TranscriptsDisabled, NoTranscriptFound, VideoUnavailable):
        return "", "missing"
    except Exception as e:
        return "", f"error:{type(e).__name__}"


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_FILE}")

    raw_text = INPUT_FILE.read_text(encoding="utf-8").strip()
    if not raw_text:
        raise ValueError(f"Input file is empty: {INPUT_FILE}")

    videos = json.loads(raw_text)

    output = []
    for i, video in enumerate(videos, start=1):
        video_id = video.get("video_id", "")
        title = video.get("title", "")

        print(f"[{i}/{len(videos)}] Fetching transcript: {title}")

        transcript_text, transcript_source = fetch_transcript_for_video(video_id)

        item = dict(video)
        item["transcript"] = transcript_text
        item["transcript_source"] = transcript_source
        output.append(item)

        time.sleep(1)

    OUTPUT_FILE.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"Saved transcripts to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()