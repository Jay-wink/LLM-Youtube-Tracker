import json
import os
import re
import time
from pathlib import Path
from collections import Counter

from dotenv import load_dotenv
from google import genai

ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = ROOT / "data" / "videos_with_transcripts.json"
OUTPUT_FILE = ROOT / "data" / "site_data.json"

BATCH_SIZE = 3
MAX_VIDEOS_TO_ENRICH = 9  # 可按需调整


def log(msg: str) -> None:
    print(msg, flush=True)


def clean_json_text(text: str) -> str:
    text = text.strip()

    if text.startswith("```"):
        parts = text.split("```")
        if len(parts) >= 2:
            text = parts[1].strip()
            if text.startswith("json"):
                text = text[4:].strip()

    return text.strip()


def generate_with_retry(
    client,
    prompt: str,
    model: str = "gemini-2.5-flash",
    max_retries: int = 5,
    initial_delay: float = 2.0,
) -> str:
    delay = initial_delay

    for attempt in range(1, max_retries + 1):
        try:
            log(f"[Gemini] Attempt {attempt}/{max_retries}")
            response = client.models.generate_content(
                model=model,
                contents=prompt,
            )
            text = response.text or ""
            log(f"[Gemini] Success on attempt {attempt}")
            return text.strip()

        except Exception as e:
            error_text = str(e)

            if "429" in error_text or "RESOURCE_EXHAUSTED" in error_text:
                log(f"[Gemini] Rate limit hit on attempt {attempt}")

                match = re.search(r"retry in (\d+)", error_text, re.IGNORECASE)
                wait_time = int(match.group(1)) + 2 if match else int(delay)

                log(f"[Gemini] Waiting {wait_time} seconds due to 429...")
                time.sleep(wait_time)
                delay *= 2

            elif "503" in error_text or "UNAVAILABLE" in error_text:
                log(f"[Gemini] Temporary overload on attempt {attempt}")
                if attempt == max_retries:
                    raise
                log(f"[Gemini] Waiting {int(delay)} seconds due to 503...")
                time.sleep(delay)
                delay *= 2

            else:
                log(f"[Gemini] Non-retryable error: {e}")
                raise

    raise RuntimeError("Unexpected retry flow reached.")


def chunk_list(items, batch_size):
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


def looks_llm_related(video: dict) -> bool:
    text = f"{video.get('title', '')} {video.get('description', '')}".lower()
    keywords = [
        "ai", "llm", "gpt", "claude", "gemini", "model",
        "agent", "fine-tuning", "anthropic", "openai"
    ]
    return any(keyword in text for keyword in keywords)


def truncate_transcript(text: str, max_chars: int = 3500) -> str:
    text = (text or "").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "..."


def build_batch_prompt(videos_batch: list[dict]) -> str:
    blocks = []

    for video in videos_batch:
        transcript = truncate_transcript(video.get("transcript", ""))

        block = f"""
VIDEO_ID: {video.get("video_id", "")}
TITLE: {video.get("title", "")}
CHANNEL: {video.get("channel_name", "")}
TRANSCRIPT:
{transcript}
""".strip()

        blocks.append(block)

    joined = "\n\n---\n\n".join(blocks)

    prompt = f"""
Return ONLY valid JSON.
Do not include markdown fences.
Do not include any explanation text.

You are given multiple YouTube videos about LLMs and AI.
Return a JSON array with one object per input video.

Use exactly this schema:
[
  {{
    "video_id": "string",
    "speaker": "string",
    "topics": ["topic1", "topic2", "topic3"],
    "summary": "string",
    "channel_style": "ai-news | research-discussion | tutorial | product-commentary | developer-education",
    "llm_relevance": "high | medium | low"
  }}
]

Rules:
- Preserve the exact VIDEO_ID.
- Return one object for every input video.
- topics should contain 3 to 6 concise topic tags.
- summary should be 1 to 2 concise sentences.
- speaker should be the main host or main speaker if inferable.
- channel_style should be the best single label.
- llm_relevance should indicate how central LLM/AI topics are.
- Do not omit any video.

Videos:
{joined}
"""
    return prompt


def enrich_video_batch(client, videos_batch: list[dict]) -> list[dict]:
    prompt = build_batch_prompt(videos_batch)
    log(f"[Batch] Sending batch with {len(videos_batch)} videos to Gemini...")

    raw_output = generate_with_retry(client, prompt)
    cleaned = clean_json_text(raw_output)

    try:
        parsed = json.loads(cleaned)
        if not isinstance(parsed, list):
            raise ValueError("Gemini output is not a JSON list")

        log(f"[Batch] Parsed {len(parsed)} results from Gemini")
        return parsed

    except Exception as e:
        log(f"[Batch] JSON parsing failed: {e}")
        log(f"[Batch] Raw output preview: {cleaned[:500]}")

        fallback = []
        for video in videos_batch:
            fallback.append({
                "video_id": video.get("video_id", ""),
                "speaker": "",
                "topics": [],
                "summary": "Batch parsing failed for this item.",
                "channel_style": "",
                "llm_relevance": ""
            })
        return fallback


def merge_batch_results(original_batch: list[dict], batch_results: list[dict]) -> list[dict]:
    result_map = {
        item.get("video_id", ""): item
        for item in batch_results
    }

    merged = []
    for video in original_batch:
        video_id = video.get("video_id", "")
        llm_fields = result_map.get(video_id, {
            "video_id": video_id,
            "speaker": "",
            "topics": [],
            "summary": "Missing batch result.",
            "channel_style": "",
            "llm_relevance": ""
        })

        item = dict(video)
        item.update(llm_fields)
        merged.append(item)

    return merged


def build_channel_relationship(client, videos: list[dict]) -> str:
    seen = set()
    summaries = []

    for video in videos:
        channel = video.get("channel_name", "")
        if channel in seen:
            continue
        seen.add(channel)

        summaries.append(
            f"Channel: {channel}\n"
            f"Style: {video.get('channel_style', '')}\n"
            f"Topics: {', '.join(video.get('topics', []))}\n"
            f"Summary: {video.get('summary', '')}\n"
        )

    prompt = f"""
Write a short landscape overview of how these YouTube channels relate to each other in the LLM ecosystem.

Focus on:
- the role each channel plays
- whether it is news-oriented, research-oriented, or practical/tooling-oriented
- how they complement each other

Keep it under 90 words.
Do not use bullet points.

Channel information:
{chr(10).join(summaries)}
"""
    return generate_with_retry(client, prompt)


def build_top_topics(videos: list[dict], top_k: int = 6) -> list[str]:
    counter = Counter()

    for video in videos:
        for topic in video.get("topics", []):
            topic = topic.strip()
            if topic:
                counter[topic] += 1

    return [topic for topic, _ in counter.most_common(top_k)]


def main():
    load_dotenv()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("Missing GEMINI_API_KEY in environment")

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_FILE}")

    raw_text = INPUT_FILE.read_text(encoding="utf-8").strip()
    if not raw_text:
        raise ValueError(f"Input file is empty: {INPUT_FILE}")

    videos = json.loads(raw_text)
    log(f"[Main] Loaded {len(videos)} videos from {INPUT_FILE}")

    # 只保留有 transcript 的视频
    videos = [v for v in videos if v.get("transcript", "").strip()]
    log(f"[Main] {len(videos)} videos have transcripts")

    # 只保留看起来和 LLM 相关的视频
    videos = [v for v in videos if looks_llm_related(v)]
    log(f"[Main] {len(videos)} videos look LLM-related")

    # 限制本次 enrichment 数量
    videos = videos[:MAX_VIDEOS_TO_ENRICH]
    log(f"[Main] Selected {len(videos)} videos for enrichment")

    if not videos:
        site_data = {
            "channel_relationship": "No videos available for enrichment.",
            "top_topics": [],
            "videos": []
        }
        OUTPUT_FILE.write_text(
            json.dumps(site_data, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        log(f"[Main] No videos selected. Wrote empty site data to {OUTPUT_FILE}")
        return

    client = genai.Client(api_key=api_key)

    batches = list(chunk_list(videos, BATCH_SIZE))
    log(f"[Main] Split into {len(batches)} batch(es) of size up to {BATCH_SIZE}")

    enriched_videos = []

    for idx, batch in enumerate(batches, start=1):
        log(f"[Main] Processing batch {idx}/{len(batches)}")
        batch_results = enrich_video_batch(client, batch)
        merged_batch = merge_batch_results(batch, batch_results)
        enriched_videos.extend(merged_batch)

        # 给免费额度一点喘息时间
        if idx < len(batches):
            log("[Main] Sleeping 12 seconds before next batch...")
            time.sleep(12)

    log("[Main] Building channel relationship overview...")
    channel_relationship = build_channel_relationship(client, enriched_videos)

    log("[Main] Building top topics...")
    top_topics = build_top_topics(enriched_videos)

    site_data = {
        "channel_relationship": channel_relationship,
        "top_topics": top_topics,
        "videos": enriched_videos
    }

    OUTPUT_FILE.write_text(
        json.dumps(site_data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    log(f"[Main] Saved site data to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()