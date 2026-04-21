import json
from pathlib import Path
from datetime import datetime, UTC

ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = ROOT / "data" / "site_data.json"
OUTPUT_FILE = ROOT / "output" / "index.html"


def html_escape(text) -> str:
    if text is None:
        return ""
    text = str(text)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def main():
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Data file not found: {DATA_FILE}")

    raw_text = DATA_FILE.read_text(encoding="utf-8").strip()
    if not raw_text:
        raise ValueError(f"Data file is empty: {DATA_FILE}")

    site_data = json.loads(raw_text)
    videos = site_data.get("videos", [])
    channel_relationship = site_data.get("channel_relationship", "")
    top_topics = site_data.get("top_topics", [])

    updated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

    topic_badges = "".join(
        f'<span class="badge">{html_escape(topic)}</span>'
        for topic in top_topics
    )

    rows = []
    for v in videos:
        topics = v.get("topics", [])
        topics_text = ", ".join(topics) if isinstance(topics, list) else str(topics)

        rows.append(f"""
        <tr>
            <td>{html_escape(v.get("published_at", ""))}</td>
            <td>{html_escape(v.get("channel_name", ""))}</td>
            <td><a href="{html_escape(v.get("url", "#"))}" target="_blank">{html_escape(v.get("title", ""))}</a></td>
            <td>{html_escape(v.get("speaker", ""))}</td>
            <td>{html_escape(topics_text)}</td>
            <td>{html_escape(v.get("channel_style", ""))}</td>
            <td>{html_escape(v.get("llm_relevance", ""))}</td>
            <td>{html_escape(v.get("summary", ""))}</td>
        </tr>
        """)

    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>LLM YouTube Tracker</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                max-width: 1400px;
                margin: 40px auto;
                padding: 0 20px;
                line-height: 1.6;
                background: #ffffff;
                color: #222;
            }}
            h1 {{
                margin-bottom: 8px;
            }}
            h2 {{
                margin-top: 36px;
                margin-bottom: 12px;
            }}
            .meta {{
                color: #666;
                margin-bottom: 20px;
            }}
            .overview-box {{
                background: #f8f9fb;
                border: 1px solid #e2e5ea;
                border-radius: 10px;
                padding: 18px;
                margin: 18px 0 24px 0;
            }}
            .badge {{
                display: inline-block;
                background: #eef3ff;
                color: #1f4ea3;
                border: 1px solid #cddcff;
                border-radius: 999px;
                padding: 6px 10px;
                margin: 4px 8px 4px 0;
                font-size: 14px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 12px;
                text-align: left;
                vertical-align: top;
            }}
            th {{
                background-color: #f5f5f5;
            }}
            tr:nth-child(even) {{
                background-color: #fafafa;
            }}
            a {{
                color: #1565c0;
                text-decoration: none;
            }}
            a:hover {{
                text-decoration: underline;
            }}
            .note {{
                margin-top: 16px;
                color: #555;
            }}
        </style>
    </head>
    <body>
        <h1>LLM YouTube Landscape Tracker</h1>
        <p class="meta">Last updated: {updated_at}</p>
        <p>Tracked videos in this demo: {len(videos)}</p>

        <h2>Channel Landscape Overview</h2>
        <div class="overview-box">
            {html_escape(channel_relationship)}
        </div>

        <h2>Top Topics</h2>
        <div class="overview-box">
            {topic_badges if topic_badges else "<p>No topics available.</p>"}
        </div>

        <h2>Video Table</h2>
        <table>
            <thead>
                <tr>
                    <th>Published</th>
                    <th>Channel</th>
                    <th>Video</th>
                    <th>Speaker</th>
                    <th>Topics</th>
                    <th>Channel Style</th>
                    <th>LLM Relevance</th>
                    <th>Summary</th>
                </tr>
            </thead>
            <tbody>
                {''.join(rows)}
            </tbody>
        </table>

        <p class="note">
            Phase 2 adds channel-level landscape analysis and top-topic aggregation on top of transcript-based LLM extraction.
        </p>
    </body>
    </html>
    """

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(html, encoding="utf-8")
    print(f"Built site: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()