# Exercise 3 — Real-Time Sentiment Analysis Pipeline

## Context

This exercise extends the Kafka-based distributed chatbot system introduced in Exercise 1 and
Exercise 2. Where Exercise 1 built a microservices chatbot and Exercise 2 introduced an LLM
processing pipeline, Exercise 3 introduces a **real-time sentiment analysis pipeline for streaming
user reviews**.

The system processes reviews asynchronously: a producer fires them into Kafka, a processor
enriches each review through LLM analysis and self-correction, and an analytics consumer
prints live insights to the terminal.

---

## 1. Architecture Overview

```
                        ┌─────────────────────────────┐
                        │         producer.ts          │
                        │   (CLI — accepts user input) │
                        └──────────────┬──────────────┘
                                       │  ReviewEvent
                                       ▼
                            ┌─────────────────────┐
                            │  raw-reviews-topic   │
                            └──────────┬──────────┘
                                       │
                        ┌──────────────▼──────────────┐
                        │         processor.ts         │
                        │  1. LLM Router               │
                        │  2. LLM Analysis             │
                        │  3. Self-Correction Check    │
                        └──────────────┬──────────────┘
                                       │  ReviewInsightEvent
                                       ▼
                         ┌───────────────────────────┐
                         │  processed-insights-topic  │
                         └─────────────┬─────────────┘
                                       │
                        ┌──────────────▼──────────────┐
                        │         analytics.ts         │
                        │  Prints real-time insights   │
                        │  Tracks running score avg    │
                        └─────────────────────────────┘
```

**Key design decisions:**
- The producer is fire-and-forget — it does not wait for processing results.
- The processor is the only Kafka consumer on `raw-reviews-topic`.
- The analytics consumer is decoupled; it reads only from `processed-insights-topic`.
- LLM calls are synchronous within the processor before publishing.

---

## 2. Kafka Topics

| Topic | Partitions | Key | Purpose |
|---|---|---|---|
| `raw-reviews-topic` | 1 | `reviewId` | Inbound raw review text from producer |
| `processed-insights-topic` | 1 | `reviewId` | Enriched insights after LLM analysis |

**Message formats:**

`raw-reviews-topic` — `ReviewEvent`
```json
{
  "reviewId": "uuid-v4",
  "text": "The battery life is great but the screen is dim.",
  "timestamp": "2026-03-09T12:00:00.000Z"
}
```

`processed-insights-topic` — `ReviewInsightEvent`
```json
{
  "reviewId": "uuid-v4",
  "originalText": "The battery life is great but the screen is dim.",
  "summary": "Positive battery life, negative screen brightness.",
  "overall_sentiment": "Mixed",
  "score": 6,
  "aspects": [
    { "aspect": "battery life", "sentiment": "Positive" },
    { "aspect": "screen brightness", "sentiment": "Negative" }
  ],
  "corrected": false,
  "timestamp": "2026-03-09T12:00:01.200Z"
}
```

---

## 3. Event Pipeline

```
[User types review in terminal]
            │
            ▼
      producer.ts
      - Generate UUID reviewId
      - Wrap in ReviewEvent
      - Publish to raw-reviews-topic (fire-and-forget)
            │
            ▼
   [raw-reviews-topic]
            │
            ▼
      processor.ts — Step 1: LLM Router
      - Call LLM with REVIEW_ROUTER_PROMPT
      - If intent != "analyzeReview" → skip (log and commit offset)
      - If intent == "analyzeReview" → proceed
            │
            ▼
      processor.ts — Step 2: LLM Analysis
      - Call LLM with REVIEW_ANALYZER_PROMPT
      - Parse JSON response into ReviewInsightEvent
            │
            ▼
      processor.ts — Step 3: Self-Correction
      - If score < 4 AND overall_sentiment == "Positive":
          call LLM again with SELF_CORRECTION_PROMPT
          parse corrected result
          set corrected = true
      - Else: proceed with original result
            │
            ▼
      Publish ReviewInsightEvent to processed-insights-topic
            │
            ▼
   [processed-insights-topic]
            │
            ▼
      analytics.ts
      - Print formatted insight to stdout
      - Update running score average
```

---

## 4. Service Responsibilities

### producer.ts

**Role:** CLI entry point. Accepts review text and publishes it to Kafka.

**Behavior:**
- Reads review text from stdin (one review per line, or single arg mode).
- Generates a `reviewId` using `crypto.randomUUID()`.
- Wraps the text in a `ReviewEvent` and publishes to `raw-reviews-topic`.
- Uses fire-and-forget semantics (`acks: 0` or no await on delivery confirmation).
- Logs `[producer] Sent review <reviewId>` after each publish.
- Does not wait for processing results.

**Configuration:**
```
KAFKA_BROKER=localhost:9092
REVIEW_PRODUCER_CLIENT_ID=review-producer
```

---

### processor.ts

**Role:** The core processing service. Enriches reviews with LLM analysis and self-corrects
inconsistent results before publishing.

**Behavior:**

1. **Consume** from `raw-reviews-topic`.
2. **Route:** Call LLM with `REVIEW_ROUTER_PROMPT`. If returned intent is not `"analyzeReview"`,
   log the skip reason and commit the offset without publishing downstream.
3. **Analyze:** Call LLM with `REVIEW_ANALYZER_PROMPT`. Parse the returned JSON into a
   `ReviewInsightEvent`.
4. **Self-correct:** If `score < 4` AND `overall_sentiment === "Positive"`, call LLM with
   `SELF_CORRECTION_PROMPT`. Use the corrected result and set `corrected: true`.
5. **Publish** the final `ReviewInsightEvent` to `processed-insights-topic`.
6. Commit the Kafka offset only after successful publish.

**Error handling:**
- If LLM JSON parsing fails, log the error and skip publishing (do not crash the consumer loop).
- If the LLM call itself throws, log and skip — do not retry in this exercise.

**Configuration:**
```
KAFKA_BROKER=localhost:9092
PROCESSOR_CLIENT_ID=review-processor
PROCESSOR_GROUP_ID=review-processor-group
```

---

### analytics.ts

**Role:** Real-time analytics display. Consumes processed insights and prints formatted output.

**Behavior:**
- Consumes from `processed-insights-topic` (separate consumer group from processor).
- For each message, prints:
  ```
  ─────────────────────────────
  New Insight Received!
  ID:        <reviewId>
  Score:     <score>/10
  Sentiment: <overall_sentiment>
  Summary:   <summary>
  Aspects:
    • <aspect>: <sentiment>
    • <aspect>: <sentiment>
  Corrected: <yes | no>
  ─────────────────────────────
  ```
- **(Bonus)** Maintains a running average of all scores seen in the current session and prints
  it after each message:
  ```
  Running Average Score: 7.3/10  (over 12 reviews)
  ```

**Configuration:**
```
KAFKA_BROKER=localhost:9092
ANALYTICS_CLIENT_ID=analytics-consumer
ANALYTICS_GROUP_ID=analytics-group
```

---

## 5. Prompt Definitions

### REVIEW_ROUTER_PROMPT

**Technique:** Zero-shot classification

**Purpose:** Determine whether a message is a product review that should be analyzed, or
something else (spam, test input, greeting, etc.).

```
You are a message classifier for a review processing pipeline.

Classify the following user message into one of these intents:
- analyzeReview   : The message is a product or service review containing an opinion.
- ignore          : The message is not a review (spam, greeting, test input, question, etc.).

Respond with valid JSON only. No explanation. No markdown.

Schema:
{
  "intent": "analyzeReview" | "ignore",
  "reason": string
}

Message:
"""
{{userInput}}
"""
```

---

### REVIEW_ANALYZER_PROMPT

**Technique:** Structured JSON output

**Purpose:** Extract structured sentiment insights from review text.

```
You are a sentiment analysis engine for product reviews.

Analyze the following review and return structured insights.

Rules:
- Respond with valid JSON only. No explanation. No markdown fences.
- score must be an integer between 1 and 10.
- overall_sentiment must be exactly one of: "Positive", "Negative", "Mixed", "Neutral".
- aspects is an array of { aspect: string, sentiment: "Positive" | "Negative" | "Neutral" }.
- summary must be one sentence, maximum 20 words.

Schema:
{
  "summary": string,
  "overall_sentiment": "Positive" | "Negative" | "Mixed" | "Neutral",
  "score": number,
  "aspects": [
    { "aspect": string, "sentiment": "Positive" | "Negative" | "Neutral" }
  ]
}

Review:
"""
{{reviewText}}
"""
```

---

### SELF_CORRECTION_PROMPT

**Technique:** Self-correction / reflective prompting

**Purpose:** Resolve the logical inconsistency when `score < 4` but `overall_sentiment` is
`"Positive"`. A score below 4 with a positive sentiment is contradictory. The LLM is asked to
re-evaluate and produce a consistent result.

```
You previously analyzed a product review and returned this result:

{{previousResult}}

There is a logical inconsistency in this result:
- The score is {{score}}/10, which indicates a poor or very poor review.
- Yet the overall_sentiment is "Positive", which contradicts a low score.

Please re-analyze the original review and return a corrected, consistent result.

Rules:
- Respond with valid JSON only. No explanation. No markdown fences.
- The corrected score and sentiment must be logically consistent.
- Use the same schema as before.

Original review:
"""
{{reviewText}}
"""
```

---

## 6. Implementation Plan

### Step 1 — Project scaffold

- Create `exercise3/` directory.
- Copy `docker-compose.yml` from Exercise 2 (same Kafka KRaft setup).
- Create `package.json` with `kafkajs` and `bun-types` dependencies.
- Create `tsconfig.json` with strict mode.

### Step 2 — Shared types and topics

Create `exercise3/shared/topics.ts`:
```typescript
export const TOPICS = {
  RAW_REVIEWS: "raw-reviews-topic",
  PROCESSED_INSIGHTS: "processed-insights-topic",
} as const;
```

Create `exercise3/shared/types/events.ts` with:
- `ReviewEvent` interface
- `ReviewInsightEvent` interface
- `AspectSentiment` interface
- `RouterDecision` interface

### Step 3 — Kafka client utilities

Create `exercise3/shared/kafka/client.ts`:
- `createProducer(clientId)` — returns connected KafkaJS producer
- `createConsumer(groupId, clientId)` — returns connected KafkaJS consumer

### Step 4 — Prompt definitions

Create `exercise3/prompts/prompts.ts` with the three prompt builder functions:
- `reviewRouterPrompt(userInput: string): string`
- `reviewAnalyzerPrompt(reviewText: string): string`
- `selfCorrectionPrompt(reviewText: string, previousResult: string, score: number): string`

### Step 5 — LLM stub

Create `exercise3/llm/llmClient.ts`:
```typescript
export async function callLLM(prompt: string): Promise<string> {
  // TODO: replace with real LLM API call
  throw new Error("LLM not implemented");
}
```

Add fallback logic directly in processor.ts for each call site that catches the error and
returns a deterministic stub response, so the pipeline runs end-to-end without a real LLM.

### Step 6 — producer.ts

Implement `exercise3/producer.ts` per responsibilities above.

### Step 7 — processor.ts

Implement `exercise3/processor.ts` per responsibilities above, in order:
1. Consumer setup
2. Router call + skip logic
3. Analyzer call + JSON parse
4. Self-correction check
5. Publish to `processed-insights-topic`

### Step 8 — analytics.ts

Implement `exercise3/analytics.ts` per responsibilities above.
Add bonus running average tracker as a module-level variable.

### Step 9 — Topic creation script

Create `exercise3/topics.sh`:
```bash
kafka-topics.sh --create --topic raw-reviews-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics.sh --create --topic processed-insights-topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

### Step 10 — README

Document startup sequence and example session.

---

## 7. End-to-End Execution Scenario

### Setup

```bash
# Terminal 1 — Start Kafka
cd exercise3
docker compose up -d

# Create topics
bash topics.sh
```

### Run Services

```bash
# Terminal 2 — Start analytics consumer
bun run analytics.ts

# Terminal 3 — Start processor
bun run processor.ts

# Terminal 4 — Run producer (send a review)
bun run producer.ts
```

### Example Session

**Terminal 4 (producer):**
```
> The keyboard feels premium and typing is smooth, but the trackpad is unresponsive and the battery drains fast.
[producer] Sent review a3f1c2d4-...
```

**Terminal 3 (processor — internal logs):**
```
[processor] Received review a3f1c2d4-...
[processor] Router decision: analyzeReview
[processor] LLM analysis complete — score: 5, sentiment: Mixed
[processor] No self-correction needed.
[processor] Published insight for a3f1c2d4-...
```

**Terminal 2 (analytics):**
```
─────────────────────────────
New Insight Received!
ID:        a3f1c2d4-...
Score:     5/10
Sentiment: Mixed
Summary:   Good keyboard but poor trackpad and battery life.
Aspects:
  • keyboard feel: Positive
  • trackpad responsiveness: Negative
  • battery life: Negative
Corrected: no
─────────────────────────────
Running Average Score: 5.0/10  (over 1 reviews)
```

### Self-Correction Scenario

**Producer:**
```
> Worst product I have ever used. Completely broken out of the box.
[producer] Sent review b7e9a1f2-...
```

**Processor (internal logs):**
```
[processor] Received review b7e9a1f2-...
[processor] Router decision: analyzeReview
[processor] LLM analysis complete — score: 2, sentiment: Positive
[processor] Inconsistency detected (score=2, sentiment=Positive). Running self-correction...
[processor] Self-correction complete — score: 1, sentiment: Negative
[processor] Published corrected insight for b7e9a1f2-...
```

**Analytics:**
```
─────────────────────────────
New Insight Received!
ID:        b7e9a1f2-...
Score:     1/10
Sentiment: Negative
Summary:   Product arrived broken and is completely unusable.
Aspects:
  • product quality: Negative
  • out-of-box experience: Negative
Corrected: yes
─────────────────────────────
Running Average Score: 3.0/10  (over 2 reviews)
```

### Non-Review Routing (Router Skip)

**Producer:**
```
> hello
[producer] Sent review c2d4e6f8-...
```

**Processor:**
```
[processor] Received review c2d4e6f8-...
[processor] Router decision: ignore (reason: "Message is a greeting, not a review")
[processor] Skipping — not a review.
```

**Analytics:** *(no output — nothing published to processed-insights-topic)*

---

## File Structure

```
exercise3/
├── shared/
│   ├── kafka/
│   │   └── client.ts          # createProducer / createConsumer utilities
│   ├── topics.ts              # TOPICS constants
│   └── types/
│       └── events.ts          # ReviewEvent, ReviewInsightEvent, AspectSentiment
├── prompts/
│   └── prompts.ts             # reviewRouterPrompt, reviewAnalyzerPrompt, selfCorrectionPrompt
├── llm/
│   └── llmClient.ts           # callLLM stub
├── producer.ts                # CLI producer
├── processor.ts               # Core processing service
├── analytics.ts               # Real-time analytics consumer
├── docker-compose.yml         # Kafka KRaft (reuse from exercise2)
├── topics.sh                  # Topic creation script
├── package.json
├── tsconfig.json
└── README.md
```
