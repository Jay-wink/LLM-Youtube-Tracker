import json
import os
import time
import re
from pathlib import Path
from collections import Counter

from dotenv import load_dotenv
from google import genai

ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = ROOT / "data" / "videos_with_transcripts.json"
OUTPUT_FILE = ROOT / "data" / "site_data.json"


def clean_json_text(text: str) -> str:
    text = text.strip()

    if text.startswith("```"):
        parts = text.split("```")
        if len(parts) >= 2:
            text = parts[1].strip()
            if text.startswith("json"):
                text = text[4:].strip()

    return text.strip()


def generate_with_retry(client, prompt: str,
                        model: str = "gemini-3-flash-preview",
                        max_retries: int = 5,
                        initial_delay: float = 2.0) -> str:
    delay = initial_delay

    for attempt in range(1, max_retries + 1):
        try:
            response = client.models.generate_content(
                model=model,
                contents=prompt,
            )
            return response.text.strip()

        except Exception as e:
            error_text = str(e)

            if "429" in error_text or "RESOURCE_EXHAUSTED" in error_text:
                print(f"[Attempt {attempt}/{max_retries}] Rate limit hit.")
                match = re.search(r"retry in (\d+)", error_text, re.IGNORECASE)
                wait_time = int(match.group(1)) + 2 if match else delay
                print(f"Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                delay *= 2

            elif "503" in error_text or "UNAVAILABLE" in error_text:
                print(f"[Attempt {attempt}/{max_retries}] Temporary Gemini overload.")
                if attempt == max_retries:
                    raise
                time.sleep(delay)
                delay *= 2
            else:
                raise

    raise RuntimeError("Unexpected retry flow reached.")


def enrich_video(client, video: dict) -> dict:
    transcript = video.get("transcript", "").strip()

    prompt = f"""
Return ONLY valid JSON.
Do not include markdown fences.
Do not include explanation text.

Use exactly this schema:
{{
  "speaker": "string",
  "topics": ["topic1", "topic2"],
  "summary": "string",
  "channel_style": "ai-news | research-discussion | tutorial | product-commentary | developer-education",
  "llm_relevance": "high | medium | low"
}}

Guidelines:
- speaker should be the main host or speaker if inferable from the content.
- topics should contain 3 to 6 short topic tags.
- summary should be 1 to 2 concise sentences.
- channel_style should choose the single best label.
- llm_relevance should describe how central LLM/AI topics are in this video.

Video title:
{video.get("title", "")}

Channel:
{video.get("channel_name", "")}

Transcript/content summary:
{transcript}
"""

    raw_output = generate_with_retry(client, prompt)
    cleaned = clean_json_text(raw_output)

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        parsed = {
            "speaker": "",
            "topics": [],
            "summary": cleaned,
            "channel_style": "",
            "llm_relevance": ""
        }

    enriched = dict(video)
    enriched.update(parsed)
    return enriched


def build_channel_relationship(client, videos: list[dict]) -> str:
    channel_summaries = []
    seen = set()

    for v in videos:
        channel = v.get("channel_name", "")
        if channel in seen:
            continue
        seen.add(channel)

        channel_summaries.append(
            f"Channel: {channel}\n"
            f"Style: {v.get('channel_style', '')}\n"
            f"Example topics: {', '.join(v.get('topics', []))}\n"
            f"Example summary: {v.get('summary', '')}\n"
        )

    prompt = f"""
Write a short landscape overview of how these YouTube channels relate to each other in the LLM ecosystem.

Focus on:
- what role each channel plays
- whether it is more news-oriented, research-oriented, or practical/tooling-oriented
- how they complement each other

Keep it under 90 words.
Do not use bullet points.

Channel information:
{chr(10).join(channel_summaries)}
"""

    return generate_with_retry(client, prompt)


def build_top_topics(videos: list[dict], top_k: int = 6) -> list[str]:
    counter = Counter()

    for v in videos:
        for topic in v.get("topics", []):
            topic = topic.strip()
            if topic:
                counter[topic] += 1

    return [topic for topic, _ in counter.most_common(top_k)]


def main():
    load_dotenv()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("Missing GEMINI_API_KEY in .env")

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_FILE}")

    raw_text = INPUT_FILE.read_text(encoding="utf-8").strip()
    if not raw_text:
        raise ValueError(f"Input file is empty: {INPUT_FILE}")

    videos = json.loads(raw_text)
    client = genai.Client(api_key=api_key)

    enriched_videos = []
    for i, video in enumerate(videos, start=1):
        print(f"[{i}/{len(videos)}] Enriching: {video.get('title', '')}")
        enriched = enrich_video(client, video)
        enriched_videos.append(enriched)
        time.sleep(12)

    print("Building channel relationship overview...")
    channel_relationship = build_channel_relationship(client, enriched_videos)

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

    print(f"Saved site data to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()