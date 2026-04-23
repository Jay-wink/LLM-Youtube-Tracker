# LLM YouTube Landscape Tracker

## 1. Problem Statement

The rapid growth of Large Language Models (LLMs) has led to an explosion of online content, particularly on platforms such as YouTube. However, most existing aggregation methods rely only on metadata such as titles, thumbnails, or view counts, which often fail to reflect the actual technical content discussed in videos.

The objective of this project is to build an automated system that:

* Tracks high-impact YouTube channels related to LLMs
* Extracts **actual spoken content** from videos (via transcripts)
* Uses LLMs to analyze and structure the information
* Continuously updates a web interface to reflect the evolving landscape

This ensures that each entry represents what creators **actually say**, rather than superficial metadata.

---

## 2. Methodology

The system is designed as an automated pipeline with three main stages:

### 2.1 Data Collection

We maintain a dynamic list of YouTube channels:

* `channels.csv`: curated + promoted channels
* `channel_candidates.csv`: automatically discovered candidates

The pipeline is shown below, which performs:
1. Fetch recent videos via YouTube Data API
2. Store metadata (title, description, publish date, etc.)

Additionally, new channels are discovered weekly using keyword-based search and scoring heuristics.

---

### 2.2 Transcript Acquisition

To ensure content-level understanding, transcripts are collected using a **multi-strategy fallback system**:

1. **Creator-provided transcripts**

   * Extracted from video descriptions (e.g., Rescript links, PDF transcripts)

2. **Automatic speech recognition (ASR)**

   * Audio downloaded using `yt-dlp`
   * Transcribed locally using `faster-whisper`

This fallback design ensures robustness when:

* YouTube captions are unavailable
* API requests are blocked (e.g., IP restrictions)

---

### 2.3 LLM Analysis

Each transcript is processed using an LLM to extract:

* Topics
* Summary
* Channel style classification
* LLM relevance score

This step transforms raw transcripts into structured insights suitable for analysis and visualization.

---

### 2.4 Automatic Update Pipeline

The system is fully automated using GitHub Actions:

* **Daily job**

  * Fetch new videos
  * Update transcripts
  * Run LLM enrichment
  * Rebuild and deploy the website

* **Weekly job**

  * Discover new candidate channels
  * Score and promote high-quality channels

---

### 2.5 Pipeline Overview

![Pipeline Diagram](./pipeline.png)

The pipeline consists of:

Candidate Channels → Channels → Recent Videos → Transcripts → LLM Analysis → Web Page

---

## 3. Evaluation Dataset

The dataset consists of:

* Selected high-quality LLM-related YouTube channels
* Their most recent videos (typically last 10 per channel)

Channels include:

* MattVidPro (AI news)
* Machine Learning Street Talk (research discussion)
* Claude (official model updates)
* Sebastian Raschka (educational content)

---

## 4. Evaluation Methods

We evaluate the system qualitatively based on:

1. **Transcript Quality**

   * Creator-provided transcripts (high quality)
   * ASR-generated transcripts (moderate quality)

2. **LLM Output Quality**

   * Coherence of summaries
   * Accuracy of topic extraction
   * Consistency of channel classification

3. **System Robustness**

   * Ability to handle missing transcripts
   * Fallback strategies under API/IP limitations

---

## 5. Experimental Results

The system successfully:

* Extracts transcripts for a subset of videos via creator links
* Uses ASR to recover transcripts when unavailable
* Generates structured summaries and topic tags using LLMs
* Automatically updates a live webpage via GitHub Pages

Example output includes:

* Topics: *Claude Opus, Gemini TTS, AI coding*
* Style: *AI news / research discussion*
* Summary: concise description of video content

---

## 6. Limitations and Future Work

### Current Limitations

* YouTube transcript APIs are frequently blocked (IP restrictions)
* ASR is computationally expensive on CPU
* Some creator links (e.g., dynamic pages) are not easily parsed

---

### Future Improvements

With more time, the system could be improved by:

* Using **residential proxies** to reliably access YouTube captions
* Improving transcript extraction from dynamic web pages
* Using GPU acceleration for faster ASR
* Ranking videos based on engagement trends
* Clustering topics over time for trend analysis

---

## 7. Conclusion

This project demonstrates a fully automated pipeline that moves beyond metadata-based tracking and instead analyzes the **actual spoken content** of LLM-related videos.

By combining:

* Data collection
* Robust transcript acquisition
* LLM-based analysis
* Automated deployment

we provide a scalable system for monitoring the evolving LLM ecosystem.
