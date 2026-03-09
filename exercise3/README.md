# Exercise 3 — LLM Processing Pipeline

## Overview

Exercise 3 builds a real-time review analysis system using Apache Kafka and the OpenAI API. User reviews are streamed through a Kafka pipeline, processed by an LLM that extracts structured sentiment insights, and displayed by a live analytics consumer.

The processor applies three prompt engineering techniques in sequence:

| Step | Technique | Purpose |
|---|---|---|
| 1 | Zero-Shot Classification | Decide whether the input is a product review or should be ignored |
| 2 | Structured JSON Output | Extract `summary`, `sentiment`, `score`, and `aspects` as typed JSON |
| 3 | Self-Correction | Detect and fix logical inconsistencies in the LLM output |

---

## Architecture

```
[User types a review in terminal]
              │
              ▼
         producer.ts
              │
       raw-reviews-topic
              │
              ▼
         processor.ts
         ┌──────────────────────────────────┐
         │ Step 1 — Router                  │
         │   Is this a product review?      │
         │   yes → continue  /  no → skip   │
         │                                  │
         │ Step 2 — Analyzer                │
         │   Extract structured JSON        │
         │                                  │
         │ Step 3 — Self-Correction         │
         │   score < 4 and Positive?        │
         │   → re-run LLM                   │
         └──────────────────────────────────┘
              │
    processed-insights-topic
              │
              ▼
         analytics.ts
         (real-time insights + running average score)
```

---

## Components

### producer.ts

CLI Kafka producer that reads user reviews from stdin one line at a time. Each line is wrapped in a `ReviewEvent` with a generated UUID and sent to `raw-reviews-topic`. Supports both interactive mode and single-shot mode via a command-line argument.

---

### processor.ts

Kafka consumer that processes every review through a three-step LLM pipeline.

**Step 1 — Router:** Calls the LLM with `reviewRouterPrompt` (zero-shot classification). If the message is not a product review, the event is skipped and the offset is committed without publishing downstream.

**Step 2 — Analyzer:** Calls the LLM with `reviewAnalyzerPrompt` (structured JSON output) to extract `summary`, `overall_sentiment`, `score`, and `aspects`.

**Step 3 — Self-Correction:** If `score < 4` and `overall_sentiment === "Positive"`, the inconsistency is flagged and the LLM is called again with `selfCorrectionPrompt` to produce a logically consistent result.

If the OpenAI API is unavailable, each step falls back to a deterministic stub so the pipeline continues running.

---

### analytics.ts

Kafka consumer that subscribes to `processed-insights-topic` and prints formatted insights in real time. Maintains a running average of all review scores received in the current session.

---

### prompts.ts

Defines all LLM prompt templates used by the processor.

| Function | Technique | Used in |
|---|---|---|
| `reviewRouterPrompt(text)` | Zero-Shot Classification | Step 1 — Router |
| `reviewAnalyzerPrompt(text)` | Structured JSON Output | Step 2 — Analyzer |
| `selfCorrectionPrompt(text, prev, score)` | Self-Correction | Step 3 — Corrector |

---

## Topics

| Topic | Producer | Consumer | Description |
|---|---|---|---|
| `raw-reviews-topic` | producer.ts | processor.ts | Inbound raw review text |
| `processed-insights-topic` | processor.ts | analytics.ts | Enriched insights after LLM analysis |

All messages use `reviewId` as the Kafka key.

---

## Running the Exercise

### 1. Set up environment

Create a `.env` file inside `exercise3/`:

```
OPENAI_API_KEY=your_key_here
```

### 2. Start Kafka

```bash
cd exercise3
docker compose up -d
```

### 3. Create topics

```bash
chmod +x topics.sh
bash topics.sh
```

### 4. Install dependencies

```bash
bun install
```

### 5. Start services

Open a separate terminal for each service. Start the analytics consumer first so it captures all messages.

```bash
# Terminal 1 — Analytics (start first)
bun run analytics.ts

# Terminal 2 — Processor
bun run processor.ts

# Terminal 3 — Producer (type reviews here)
bun run producer.ts
```

---

## Example Output

**Producer terminal:**
```
[producer] Connected. Type a review and press Enter. Ctrl+C to exit.

> Great pizza and excellent service, would definitely come back.
[producer] Sent review a3f1c2d4-...
```

**Processor terminal:**
```
[processor] Started. Waiting for reviews...
[processor] Received review a3f1c2d4-...: "Great pizza and excellent service..."
[processor] Routing decision: intent="analyzeReview"
[processor] Insight published for a3f1c2d4-...
```

**Analytics terminal:**
```
[analytics] Started. Listening for insights...

--------------------------------
New Insight Received!
ID:        a3f1c2d4-...
Score:     8/10
Sentiment: Positive
Summary:   Great pizza and excellent service.
--------------------------------
Average Score: 8.0/10  (over 1 review)
```

**Self-correction example:**

When the LLM returns a contradictory result (`score < 4` and `sentiment == "Positive"`):

```
[processor] Inconsistency detected for b7e9a1f2-... (score=2, sentiment=Positive). Running self-correction...
[processor] Self-correction complete — score=1, sentiment=Negative
[processor] Insight published for b7e9a1f2-...
```

**Non-review routing:**

Messages that are not product reviews are skipped entirely:

```
[processor] Received review c2d4e6f8-...: "hello"
[processor] Routing decision: intent="ignore"
[processor] Skipping c2d4e6f8-... — not a review.
```

**Sarcasm test case:**

The following Hebrew sarcastic message must be classified as `Negative` sentiment:

```
ממש תודה למארחת שגלגלה עיניים
```

This tests the model's ability to detect negative sentiment expressed through irony rather than explicit negative language.

---

## Teardown

```bash
docker compose down -v
```
