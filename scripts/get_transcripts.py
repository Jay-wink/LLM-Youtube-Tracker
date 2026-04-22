import io
import json
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
)

ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = ROOT / "data" / "videos_raw.json"
OUTPUT_FILE = ROOT / "data" / "videos_with_transcripts.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    )
}

TRANSCRIPT_HINTS = [
    "transcript",
    "full transcript",
    "pdf transcript",
    "rescript",
]

URL_PATTERN = re.compile(r"https?://[^\s)\]]+")


def log(msg: str) -> None:
    print(msg, flush=True)


def load_existing_transcripts() -> dict:
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
        video_id = (item.get("video_id") or "").strip()
        if video_id:
            existing[video_id] = item
    return existing


def should_reuse_existing(existing_item: dict) -> bool:
    if not existing_item:
        return False

    source = (existing_item.get("transcript_source") or "").strip()

    if source in {"manual_caption", "auto_caption", "creator_link", "creator_pdf", "creator_rescript"}:
        return True
    if source.startswith("caption:"):
        return True
    if source in {"missing", "request_blocked"}:
        return True
    if source.startswith("error:"):
        return True

    return False


def join_fetched_transcript(fetched_transcript) -> str:
    return " ".join(snippet.text for snippet in fetched_transcript).strip()


def clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_urls_from_description(description: str) -> list[str]:
    if not description:
        return []
    return URL_PATTERN.findall(description)


def score_transcript_link(url: str, description: str) -> int:
    score = 0
    lower_url = url.lower()
    lower_desc = description.lower()

    for hint in TRANSCRIPT_HINTS:
        if hint in lower_url:
            score += 5
        if hint in lower_desc and url in description:
            score += 2

    if lower_url.endswith(".pdf"):
        score += 4
    if "rescript" in lower_url:
        score += 4
    if "transcript" in lower_url:
        score += 3

    return score


def find_candidate_transcript_links(description: str) -> list[str]:
    urls = extract_urls_from_description(description)
    urls = sorted(set(urls), key=lambda u: score_transcript_link(u, description), reverse=True)
    return urls


def fetch_pdf_text(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()

    pdf_stream = io.BytesIO(resp.content)
    reader = PdfReader(pdf_stream)

    pages = []
    for page in reader.pages:
        text = page.extract_text() or ""
        if text.strip():
            pages.append(text)

    return clean_text(" ".join(pages))


def fetch_html_text(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text_blocks = []

    for selector in ["article", "main"]:
        for node in soup.select(selector):
            text = clean_text(node.get_text(" ", strip=True))
            if len(text) > 500:
                text_blocks.append(text)

    if not text_blocks:
        body = soup.body or soup
        text = clean_text(body.get_text(" ", strip=True))
        if text:
            text_blocks.append(text)

    text = " ".join(text_blocks)
    text = clean_text(text)

    if len(text) < 300:
        return ""

    return text


def fetch_creator_transcript_from_link(url: str) -> tuple[str, str]:
    lower_url = url.lower()

    # PDF transcript
    if lower_url.endswith(".pdf"):
        text = fetch_pdf_text(url)
        if text:
            return text, "creator_pdf"
        return "", ""

    text = fetch_html_text(url)
    if text:
        if "rescript" in lower_url:
            return text, "creator_rescript"
        return text, "creator_link"

    return "", ""


def fetch_transcript_from_description_links(description: str) -> tuple[str, str]:
    links = find_candidate_transcript_links(description)

    for url in links:
        try:
            text, source = fetch_creator_transcript_from_link(url)
            if text:
                return text, source
        except Exception:
            continue

    return "", ""


def fetch_transcript_from_youtube(ytt_api: YouTubeTranscriptApi, video_id: str) -> tuple[str, str]:
    # direct fetch
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

    # list available
    try:
        transcript_list = ytt_api.list(video_id)
    except (NoTranscriptFound, TranscriptsDisabled, VideoUnavailable):
        return "", "missing"
    except Exception as e:
        error_text = str(e)
        if "IpBlocked" in error_text or "RequestBlocked" in error_text:
            return "", "request_blocked"
        return "", f"error:list:{type(e).__name__}"

    # manual English
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

    # generated English
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

    # any available
    for transcript in transcript_list:
        try:
            fetched = transcript.fetch()
            text = join_fetched_transcript(fetched)
            if text:
                return text, f"caption:{transcript.language_code}"
        except Exception as e:
            error_text = str(e)
            if "IpBlocked" in error_text or "RequestBlocked" in error_text:
                return "", "request_blocked"
            continue

    return "", "missing"


def fetch_transcript_for_video(ytt_api: YouTubeTranscriptApi, video: dict) -> tuple[str, str]:
    description = video.get("description", "") or ""
    video_id = (video.get("video_id") or "").strip()

    # 1) creator-provided transcript links
    text, source = fetch_transcript_from_description_links(description)
    if text:
        return text, source

    # 2) youtube captions
    return fetch_transcript_from_youtube(ytt_api, video_id)


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
    log(f"[Main] Loaded {len(existing_map)} cached transcript records")

    output = []
    reused_count = 0
    fetched_count = 0

    for i, video in enumerate(videos, start=1):
        video_id = (video.get("video_id") or "").strip()
        title = video.get("title", "") or ""

        existing_item = existing_map.get(video_id)
        if existing_item and should_reuse_existing(existing_item):
            reused_item = dict(video)
            reused_item["transcript"] = existing_item.get("transcript", "")
            reused_item["transcript_source"] = existing_item.get("transcript_source", "")
            output.append(reused_item)

            reused_count += 1
            log(f"[{i}/{len(videos)}] Reusing cached result: {title} -> {reused_item['transcript_source']}")
            continue

        log(f"[{i}/{len(videos)}] Fetching transcript: {title}")

        transcript_text, transcript_source = fetch_transcript_for_video(ytt_api, video)

        item = dict(video)
        item["transcript"] = transcript_text
        item["transcript_source"] = transcript_source
        output.append(item)

        fetched_count += 1
        log(f"    -> {transcript_source}")

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