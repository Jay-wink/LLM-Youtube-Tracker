import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

CHANNELS_CSV = DATA_DIR / "channels.csv"
CANDIDATES_CSV = DATA_DIR / "channel_candidates.csv"

PROMOTION_SCORE_THRESHOLD = 0.75
PROMOTION_LLM_RATIO_THRESHOLD = 0.5


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def load_csv_rows(path: Path):
    if not path.exists():
        return []

    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def save_channels(rows):
    fieldnames = ["channel_name", "channel_id", "notes"]

    with open(CHANNELS_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "channel_name": row["channel_name"],
                "channel_id": row["channel_id"],
                "notes": row["notes"],
            })


def save_candidates(rows):
    if not rows:
        return

    fieldnames = list(rows[0].keys())

    with open(CANDIDATES_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def infer_notes(candidate_row: dict) -> str:
    """
    Convert candidate metadata into a simple notes label for channels.csv
    """
    discovered_query = candidate_row.get("discovered_query", "").lower()
    channel_name = candidate_row.get("channel_name", "").lower()

    if "anthropic" in discovered_query or "claude" in discovered_query or "anthropic" in channel_name:
        return "official-llm-channel"
    if "openai" in discovered_query or "gemini" in discovered_query:
        return "official-llm-channel"
    if "large language model" in discovered_query or "llm" in discovered_query:
        return "llm-education"
    if "ai news" in discovered_query:
        return "ai-news"

    return "auto-promoted"


def should_promote(candidate_row: dict) -> bool:
    status = candidate_row.get("status", "").strip().lower()
    score = safe_float(candidate_row.get("score", 0))
    recent_llm_ratio = safe_float(candidate_row.get("recent_llm_ratio", 0))

    return (
        status == "candidate"
        and score >= PROMOTION_SCORE_THRESHOLD
        and recent_llm_ratio >= PROMOTION_LLM_RATIO_THRESHOLD
    )


def main():
    channels = load_csv_rows(CHANNELS_CSV)
    candidates = load_csv_rows(CANDIDATES_CSV)

    existing_channel_ids = {row["channel_id"] for row in channels if row.get("channel_id")}
    promoted_count = 0

    for candidate in candidates:
        channel_id = candidate.get("channel_id", "").strip()
        channel_name = candidate.get("channel_name", "").strip()

        if not channel_id:
            continue

        if channel_id in existing_channel_ids:
            candidate["status"] = "promoted"
            continue

        if should_promote(candidate):
            notes = infer_notes(candidate)

            channels.append({
                "channel_name": channel_name,
                "channel_id": channel_id,
                "notes": notes,
            })

            existing_channel_ids.add(channel_id)
            candidate["status"] = "promoted"
            promoted_count += 1

            print(f"Promoted: {channel_name} ({channel_id})")
        else:
            # keep existing status
            pass

    # optional: sort channels alphabetically for neatness
    channels = sorted(channels, key=lambda x: x["channel_name"].lower())

    save_channels(channels)
    save_candidates(candidates)

    print(f"Promotion complete. Added {promoted_count} channels to {CHANNELS_CSV}")


if __name__ == "__main__":
    main()