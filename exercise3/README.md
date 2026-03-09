# Exercise 3 — Kafka Review Intelligence Pipeline

## Overview

Exercise 3 builds a real-time review analysis system using Apache Kafka and the OpenAI API. User reviews are streamed through a Kafka pipeline, processed by an LLM that extracts structured sentiment insights, and displayed by a live analytics consumer.

The system demonstrates three prompt engineering techniques applied in sequence:

| Technique | Step | Purpose |
|---|---|---|
| **Zero-Shot Classification** | Router | Decide whether a message is a product review or should be ignored |
| **Structured JSON Output** | Analyzer | Extract `summary`, `sentiment`, `score`, and `aspects` as typed JSON |
| **Self-Correction** | Corrector | Detect and fix logical inconsistencies in the LLM output |

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
         ┌──────────────────────────────┐
         │ 1. LLM Router                │
         │    intent == analyzeReview?  │
         │    → yes: continue           │
         │    → no:  skip               │
         │                              │
         │ 2. LLM Analyzer              │
         │    extract structured JSON   │
         │                              │
         │ 3. Self-Correction           │
         │    score < 4 + Positive?     │
         │    → re-run LLM              │
         └──────────────────────────────┘
              │
   processed-insights-topic
              │
              ▼
         analytics.ts
         (real-time insights + running average)
```

---

## Components

### producer.ts
**File:** `producer.ts`

CLI program that reads user reviews from stdin one line at a time. Each line is wrapped in a `ReviewEvent` with a generated UUID and sent to `raw-reviews-topic` using fire-and-forget semantics. The producer does not wait for processing results.

---

### processor.ts
**File:** `processor.ts`

Kafka consumer that processes every review through a three-step LLM pipeline:

1. **Router** — calls the LLM with `REVIEW_ROUTER_PROMPT` (zero-shot classification). If the message is not a product review the event is skipped and the offset is committed without publishing downstream.
2. **Analyzer** — calls the LLM with `REVIEW_ANALYZER_PROMPT` (structured JSON output) to extract `summary`, `overall_sentiment`, `score`, and `aspects`.
3. **Self-Correction** — if `score < 4` and `overall_sentiment === "Positive"`, the inconsistency is flagged and the LLM is called again with `SELF_CORRECTION_PROMPT` to produce a logically consistent result.

Publishes a `ReviewInsightEvent` to `processed-insights-topic` after all steps complete.

If the OpenAI API is unavailable, each step falls back to a deterministic stub so the pipeline continues running.

---

### analytics.ts
**File:** `analytics.ts`

Kafka consumer that subscribes to `processed-insights-topic` and prints formatted insights in real time. Maintains a running average of all review scores received in the current session.

---

## Kafka Topics

| Topic | Producer | Consumer | Description |
|---|---|---|---|
| `raw-reviews-topic` | producer.ts | processor.ts | Inbound raw review text |
| `processed-insights-topic` | processor.ts | analytics.ts | Enriched insights after LLM analysis |

All messages use `reviewId` as the Kafka message key.

---

## Prompt Templates

All prompts are defined in `prompts.ts`.

| Function | Technique | Used in step |
|---|---|---|
| `reviewRouterPrompt(text)` | Zero-Shot Classification | Step 1 — Router |
| `reviewAnalyzerPrompt(text)` | Structured JSON Output | Step 2 — Analyzer |
| `selfCorrectionPrompt(text, prev, score)` | Self-Correction | Step 3 — Corrector |

---

## Running Exercise 3

### 1. Set up environment

Create a `.env` file at the project root (if it does not already exist):

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

Open three terminals from the `exercise3/` directory.

```bash
# Terminal 1 — analytics consumer (start first to capture all insights)
bun run analytics.ts

# Terminal 2 — processor
bun run processor.ts

# Terminal 3 — producer (interactive)
bun run producer.ts
```

Type a review in Terminal 3 and press Enter. The processor enriches it through the LLM pipeline and the analytics consumer prints the result in real time.

---

## Example Output

**Terminal 3 (producer):**
```
[producer] Connected. Type a review and press Enter. Ctrl+C to exit.

> Great pizza and excellent service, would definitely come back.
[producer] Sent review a3f1c2d4-...
```

**Terminal 2 (processor):**
```
[processor] Started. Waiting for reviews...

[processor] Received review a3f1c2d4-...: "Great pizza and excellent service, would definitely come back."
[processor] Routing decision for a3f1c2d4-...: intent="analyzeReview" reason="Message is a product review containing an opinion."
[processor] Insight published for a3f1c2d4-...
```

**Terminal 1 (analytics):**
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

### Self-Correction Example

When the LLM returns a contradictory result (`score < 4` and `sentiment == "Positive"`), the processor detects the inconsistency and re-runs the LLM:

```
[processor] Inconsistency detected for b7e9a1f2-... (score=2, sentiment=Positive). Running self-correction...
[processor] Self-correction complete — score=1, sentiment=Negative
[processor] Insight published for b7e9a1f2-...
```

### Non-Review Routing

Messages that are not product reviews are skipped and never reach `processed-insights-topic`:

```
[processor] Received review c2d4e6f8-...: "hello"
[processor] Routing decision for c2d4e6f8-...: intent="ignore" reason="Message is a greeting, not a review."
[processor] Skipping c2d4e6f8-... — not a review.
```

---

## Teardown

```bash
docker compose down -v
```
