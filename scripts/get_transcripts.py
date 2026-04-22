import os
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
os.environ["OMP_NUM_THREADS"] = "1"

import io
import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from faster_whisper import WhisperModel
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = ROOT / "data" / "videos_raw.json"
OUTPUT_FILE = ROOT / "data" / "videos_with_transcripts.json"

STRATEGY_VERSION = 2

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    )
}

URL_PATTERN = re.compile(r"https?://[^\s)\]]+")


def log(msg: str) -> None:
    print(msg, flush=True)


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


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
    version = int(existing_item.get("transcript_strategy_version", 0) or 0)

    successful_sources = {
        "creator_pdf",
        "creator_link",
        "yt_dlp_asr",
        "manual_caption",
        "auto_caption",
    }
    if source.startswith("caption:") or source in successful_sources:
        return True

    if version == STRATEGY_VERSION:
        if source in {"missing", "request_blocked"}:
            return True
        if source.startswith("error:"):
            return True

    return False


def extract_urls_from_text(text: str) -> list[str]:
    if not text:
        return []
    return URL_PATTERN.findall(text)


def normalize_url(url: str) -> str:
    return url.strip().rstrip(").,]")


def find_candidate_transcript_links(description: str) -> list[str]:
    """
    Only keep links from lines that are explicitly transcript-related.
    This avoids accidentally using paper/reference links.
    """
    if not description:
        return []

    transcript_keywords = [
        "transcript",
        "full transcript",
        "pdf transcript",
        "download pdf transcript",
        "rescript",
    ]

    candidates = []

    for line in description.splitlines():
        line_clean = line.strip()
        line_lower = line_clean.lower()

        # only accept lines explicitly about transcripts
        if not any(keyword in line_lower for keyword in transcript_keywords):
            continue

        urls = [normalize_url(u) for u in extract_urls_from_text(line_clean)]
        if not urls:
            continue

        for url in urls:
            score = 0
            url_lower = url.lower()

            # label-based priority
            if "download pdf transcript" in line_lower:
                score += 100
            elif "pdf transcript" in line_lower:
                score += 90
            elif "full transcript" in line_lower:
                score += 80
            elif "rescript" in line_lower:
                score += 70
            elif "transcript" in line_lower:
                score += 60

            # url-based priority
            if url_lower.endswith(".pdf") or "/pdf" in url_lower:
                score += 30
            if "rescript" in url_lower:
                score += 20
            if "transcript" in url_lower:
                score += 10

            candidates.append((score, url))

    # deduplicate, keep highest score
    best = {}
    for score, url in candidates:
        best[url] = max(score, best.get(url, -1))

    return [u for u, _ in sorted(best.items(), key=lambda x: x[1], reverse=True)]


def fetch_pdf_text(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
    response.raise_for_status()

    pdf_bytes = io.BytesIO(response.content)
    reader = PdfReader(pdf_bytes)

    parts = []
    for page in reader.pages:
        text = page.extract_text() or ""
        if text.strip():
            parts.append(text)

    return clean_text(" ".join(parts))


def fetch_html_text(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    texts = []

    for selector in ["article", "main"]:
        for node in soup.select(selector):
            text = clean_text(node.get_text(" ", strip=True))
            if len(text) > 500:
                texts.append(text)

    if not texts:
        body = soup.body or soup
        text = clean_text(body.get_text(" ", strip=True))
        if text:
            texts.append(text)

    merged = clean_text(" ".join(texts))
    if len(merged) < 300:
        return ""

    return merged


def fetch_transcript_from_description_links(video: dict) -> tuple[str, str]:
    description = video.get("description", "") or ""
    links = find_candidate_transcript_links(description)

    if not links:
        return "", ""

    log(f"    [desc-link] transcript candidates: {links}")

    for url in links:
        try:
            lower = url.lower()

            # Prefer PDF first
            if lower.endswith(".pdf") or "/pdf" in lower:
                text = fetch_pdf_text(url)
                if text and len(text) > 300:
                    return text, "creator_pdf"
                continue

            # Then transcript / rescript pages
            text = fetch_html_text(url)
            if text and len(text) > 300:
                return text, "creator_link"

        except Exception as e:
            log(f"    [desc-link] failed for {url}: {type(e).__name__}")

    return "", ""


def download_audio_with_ytdlp(video_url: str) -> tuple[Path | None, str]:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        outtmpl = str(tmpdir_path / "%(id)s.%(ext)s")

        cmd = [
            "yt-dlp",
            "-v",
            "--js-runtimes", "deno",
            "--no-playlist",
            "-f", "bestaudio/best",
            "--output", outtmpl,
            video_url,
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            stdout = (result.stdout or "").strip()
            full_error = stderr if stderr else stdout
            return None, f"error:yt_dlp:{full_error}"

        media_files = [
            p for p in tmpdir_path.iterdir()
            if p.is_file() and p.suffix.lower() not in {".part", ".ytdl", ".json"}
        ]

        if not media_files:
            return None, "missing"

        media_files.sort(key=lambda p: p.stat().st_size, reverse=True)
        chosen = media_files[0]

        suffix = chosen.suffix or ".audio"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_out:
            tmp_out.write(chosen.read_bytes())
            stable_path = Path(tmp_out.name)

        return stable_path, "downloaded_audio"


def transcribe_audio(audio_path: Path, model: WhisperModel) -> str:
    segments, info = model.transcribe(
        str(audio_path),
        language="en",
        vad_filter=True,
        beam_size=1,
        condition_on_previous_text=False,
    )

    parts = []
    for seg in segments:
        text = (seg.text or "").strip()
        if text:
            parts.append(text)

    return clean_text(" ".join(parts))


def fetch_transcript_from_audio(video: dict, model: WhisperModel) -> tuple[str, str]:
    video_url = video.get("url", "") or ""
    if not video_url:
        return "", "missing"

    audio_path, source = download_audio_with_ytdlp(video_url)
    if audio_path is None:
        return "", source

    try:
        text = transcribe_audio(audio_path, model)
        if text and len(text) > 100:
            return text, "yt_dlp_asr"
        return "", "missing"
    finally:
        try:
            audio_path.unlink(missing_ok=True)
        except Exception:
            pass


def fetch_transcript_for_video(video: dict, model: WhisperModel) -> tuple[str, str]:
    # Tier 1: creator-provided transcript links
    text, source = fetch_transcript_from_description_links(video)
    if text:
        return text, source

    # Tier 2: local ASR from downloaded audio
    return fetch_transcript_from_audio(video, model)


def main():
    load_dotenv()

    whisper_model_size = os.getenv("WHISPER_MODEL_SIZE", "small")
    whisper_device = os.getenv("WHISPER_DEVICE", "cpu")
    whisper_compute_type = os.getenv("WHISPER_COMPUTE_TYPE", "int8")

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_FILE}")

    raw_text = INPUT_FILE.read_text(encoding="utf-8").strip()
    if not raw_text:
        raise ValueError(f"Input file is empty: {INPUT_FILE}")

    videos = json.loads(raw_text)
    existing_map = load_existing_transcripts()

    log(f"[Main] Loaded {len(videos)} videos from {INPUT_FILE}")
    log(f"[Main] Loaded {len(existing_map)} cached transcript records")
    log(f"[Main] Loading faster-whisper model: size={whisper_model_size}, device={whisper_device}, compute={whisper_compute_type}")

    model = WhisperModel(
        whisper_model_size,
        device=whisper_device,
        compute_type=whisper_compute_type,
    )

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
            reused_item["transcript_strategy_version"] = existing_item.get("transcript_strategy_version", STRATEGY_VERSION)
            output.append(reused_item)

            reused_count += 1
            log(f"[{i}/{len(videos)}] Reusing cached result: {title} -> {reused_item['transcript_source']}")
            continue

        log(f"[{i}/{len(videos)}] Fetching transcript: {title}")

        transcript_text, transcript_source = fetch_transcript_for_video(video, model)

        item = dict(video)
        item["transcript"] = transcript_text
        item["transcript_source"] = transcript_source
        item["transcript_strategy_version"] = STRATEGY_VERSION
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