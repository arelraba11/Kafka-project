# Exercise 3 — Implementation Context

This document is a persistent reference for Claude Code sessions working on Exercise 3.
It captures the full architecture, design decisions, and implementation details so future
sessions can understand the system without re-reading all source files.

---

## Project Context

Exercise 3 implements a **real-time Kafka-based review intelligence pipeline** using the
OpenAI API for LLM routing and sentiment analysis. It is the third exercise in a progressive
series:

- Exercise 1 — distributed microservices chatbot (pure regex routing)
- Exercise 2 — LLM prompt engineering pipeline (stubs, not wired to a real API)
- **Exercise 3 — real-time review intelligence pipeline (live OpenAI integration)**

Exercise 3 is fully independent from Exercises 1 and 2. It lives in the `exercise3/` directory
with its own `package.json`, `docker-compose.yml`, Kafka topics, shared types, and services.

---

## System Architecture

```
[User types a review in terminal]
              │
              ▼
         producer.ts           ← reads stdin, wraps in ReviewEvent, fire-and-forget
              │
    raw-reviews-topic           ← Kafka topic, key = reviewId
              │
              ▼
         processor.ts
         ┌──────────────────────────────────────┐
         │ Step 1: LLM Router                   │
         │   intent == "analyzeReview"?          │
         │   → no:  skip, commit offset, done   │
         │   → yes: continue                    │
         │                                      │
         │ Step 2: LLM Analyzer                 │
         │   extract summary, sentiment,        │
         │   score, aspects                     │
         │                                      │
         │ Step 3: Self-Correction (conditional)│
         │   if score < 4 AND sentiment ==      │
         │   "Positive" → re-call LLM to fix   │
         │   contradiction, set corrected=true  │
         └──────────────────────────────────────┘
              │
   processed-insights-topic     ← Kafka topic, key = reviewId
              │
              ▼
         analytics.ts           ← prints formatted insights + running average score
```

All inter-service communication goes through Kafka. No service calls another service directly.

---

## File Structure

```
exercise3/
├── kafka/
│   └── kafkaClient.ts          # createProducer() / createConsumer() utilities
├── llm/
│   └── llmClient.ts            # OpenAI client; exports generateText(prompt)
├── shared/
│   ├── topics.ts               # TOPICS constants
│   └── types.ts                # all TypeScript interfaces
├── producer.ts                 # CLI stdin producer (fire-and-forget)
├── processor.ts                # core LLM pipeline service (consumer + producer)
├── analytics.ts                # real-time insights display consumer
├── prompts.ts                  # all prompt builder functions
├── docker-compose.yml          # Kafka in KRaft mode (no Zookeeper)
├── topics.sh                   # creates both Kafka topics
├── package.json                # kafkajs, openai, dotenv, bun-types
└── tsconfig.json               # strict ES2022, bundler resolution, bun-types
```

---

## Services

### producer.ts

**Entry point:** CLI interactive script.

**Behaviour:**
- Connects a KafkaJS producer using `createProducer("review-producer")`.
- Reads from `Bun.stdin.stream()` one line at a time.
- For each non-empty line:
  - generates a UUID with `crypto.randomUUID()`
  - builds a `ReviewEvent` with `{ reviewId, text, timestamp }`
  - calls `producer.send(...)` **without `await`** — fire-and-forget, no ack wait
  - logs `[producer] Sent review <reviewId>`
- Disconnects cleanly on stdin close (Ctrl+C / EOF).

**Consumer group:** none (producer only).

**Environment variables:**
- `REVIEW_PRODUCER_CLIENT_ID` (default: `"review-producer"`)
- `KAFKA_BROKER` (default: `"localhost:9092"`)

---

### processor.ts

**Entry point:** Long-running Kafka consumer + producer.

**Behaviour per message:**

1. **Parse** — reads `message.value`, parses as `ReviewEvent`.
2. **Route** — calls `route(text)` which calls `callLLM(reviewRouterPrompt(text))` and parses
   the JSON response as `RouterDecision`. If `intent === "ignore"`, logs the skip reason and
   returns (offset committed, nothing published).
3. **Analyze** — calls `analyze(text)` which calls `callLLM(reviewAnalyzerPrompt(text))` and
   parses the JSON response as `AnalysisResult`.
4. **Self-correct** — if `result.score < 4 && result.overall_sentiment === "Positive"`, calls
   `selfCorrect(text, result)` which calls `callLLM(selfCorrectionPrompt(...))`. Sets
   `corrected = true` on the final event.
5. **Publish** — builds `ReviewInsightEvent` and calls `await producer.send(...)` to
   `processed-insights-topic`. Offset is committed only after the publish succeeds.
6. **Heartbeat** — calls `await heartbeat()` after each message to prevent consumer timeout.

**Internal call chain:**
```
route() / analyze() / selfCorrect()
    └──▶ callLLM(prompt)
              └──▶ generateText(prompt)     (llm/llmClient.ts)
                        └──▶ OpenAI API
```

**Consumer group:** `review-processor-group`

**Environment variables:**
- `PROCESSOR_CLIENT_ID` (default: `"review-processor"`)
- `PROCESSOR_GROUP_ID` (default: `"review-processor-group"`)
- `KAFKA_BROKER` (default: `"localhost:9092"`)

---

### analytics.ts

**Entry point:** Long-running Kafka consumer.

**Behaviour per message:**
- Parses `message.value` as `ReviewInsightEvent` (with a try/catch — logs error and skips on
  bad JSON, never crashes the loop).
- Calls `printInsight(event)` which:
  - increments `totalScore` and `reviewCount` (module-level state)
  - computes running average as `(totalScore / reviewCount).toFixed(1)`
  - prints a formatted block to stdout

**Output format:**
```
--------------------------------
New Insight Received!
ID:        <reviewId>
Score:     <score>/10
Sentiment: <overall_sentiment>
Summary:   <summary>
--------------------------------
Average Score: <avg>/10  (over <n> review[s])
```

**Consumer group:** `review-analytics-group` (separate from `review-processor-group`, so both
services can independently read from `processed-insights-topic`)

**Environment variables:**
- `ANALYTICS_CLIENT_ID` (default: `"analytics-consumer"`)
- `ANALYTICS_GROUP_ID` (default: `"review-analytics-group"`)
- `KAFKA_BROKER` (default: `"localhost:9092"`)

---

## Kafka Topics

| Constant | Topic name | Key | Partitions | Producer | Consumer |
|---|---|---|---|---|---|
| `TOPICS.RAW_REVIEWS` | `raw-reviews-topic` | `reviewId` | 1 | producer.ts | processor.ts |
| `TOPICS.PROCESSED_INSIGHTS` | `processed-insights-topic` | `reviewId` | 1 | processor.ts | analytics.ts |

Defined in `shared/topics.ts`:
```typescript
export const TOPICS = {
  RAW_REVIEWS: "raw-reviews-topic",
  PROCESSED_INSIGHTS: "processed-insights-topic",
} as const;
```

---

## Message Schemas

### ReviewEvent
Published to `raw-reviews-topic` by `producer.ts`.

```typescript
interface ReviewEvent {
  reviewId:  string;   // UUID v4 generated by crypto.randomUUID()
  text:      string;   // raw review text typed by the user
  timestamp: string;   // ISO 8601
}
```

### ReviewInsightEvent
Published to `processed-insights-topic` by `processor.ts`.

```typescript
interface ReviewInsightEvent {
  reviewId:          string;          // same UUID from the original ReviewEvent
  originalText:      string;          // the raw review text (passed through)
  summary:           string;          // one-sentence summary from LLM (max 20 words)
  overall_sentiment: Sentiment;       // "Positive" | "Negative" | "Mixed" | "Neutral"
  score:             number;          // integer 1–10
  aspects:           AspectSentiment[]; // per-aspect breakdown
  corrected:         boolean;         // true if self-correction was applied
  timestamp:         string;          // ISO 8601 set at publish time
}

interface AspectSentiment {
  aspect:    string;
  sentiment: "Positive" | "Negative" | "Neutral";
}
```

### RouterDecision
Internal type used by `processor.ts` to interpret the LLM router response.

```typescript
interface RouterDecision {
  intent: "analyzeReview" | "ignore";
  reason: string;
}
```

### AnalysisResult
Internal type used by `processor.ts` to interpret the LLM analyzer response.

```typescript
interface AnalysisResult {
  summary:           string;
  overall_sentiment: Sentiment;
  score:             number;
  aspects:           AspectSentiment[];
}
```

---

## LLM Integration

### llm/llmClient.ts

The only file that imports the OpenAI SDK. All LLM calls in the project go through this module.

```
processor.ts → callLLM(prompt) → generateText(prompt) → OpenAI API
```

**Implementation:**
- `import "dotenv/config"` loads `.env` on module initialisation (must run from project root
  or a directory that contains / is above the `.env` file).
- `new OpenAI({ apiKey: process.env.OPENAI_API_KEY })` — key never hardcoded.
- `generateText(prompt)` calls `chat.completions.create` with:
  - `model: "gpt-4o-mini"`
  - `temperature: 0` — deterministic output, important for JSON reliability
  - `max_tokens: 400` — sufficient for JSON responses, avoids runaway usage
- Returns the raw text string from `response.choices[0].message.content`.
- Throws `Error("Empty response from OpenAI")` if content is null/undefined.

**JSON safety:** All three prompts instruct the model to return valid JSON only with no markdown
fences and no surrounding explanation. `temperature: 0` further reduces deviation from the
schema. `processor.ts` wraps every `JSON.parse` call in a try/catch so a malformed response
activates the fallback rather than crashing the loop.

---

## Fallback Logic

Every LLM call site in `processor.ts` is wrapped in a try/catch. If `callLLM()` throws for
any reason (API key missing, network error, empty response, JSON parse failure), the processor
falls back to a deterministic stub and continues processing.

| Step | Fallback behaviour |
|---|---|
| Router (`route()`) | Returns `{ intent: "analyzeReview", reason: "fallback: assumed review" }` — all messages are treated as reviews |
| Analyzer (`analyze()`) | Returns `{ summary: "Stub summary — LLM not available.", overall_sentiment: "Neutral", score: 5, aspects: [] }` |
| Self-corrector (`selfCorrect()`) | Returns the original uncorrected `AnalysisResult` unchanged |

The pipeline never crashes due to an LLM failure. Events are always published to
`processed-insights-topic` even when running fully on stubs.

---

## Prompt Templates

All defined in `prompts.ts`. Each function takes string parameters and returns a filled
prompt string. No state, no side effects.

### `reviewRouterPrompt(userInput)`
**Technique:** Zero-shot classification

Classifies whether the input is a product/service review (`"analyzeReview"`) or something
else (`"ignore"`). Returns JSON: `{ intent, reason }`.

### `reviewAnalyzerPrompt(reviewText)`
**Technique:** Structured JSON output

Extracts sentiment insights from the review. Returns JSON:
`{ summary, overall_sentiment, score, aspects }`.

The prompt enforces strict rules: score must be 1–10 integer, sentiment must be one of four
exact string values, summary must be one sentence max 20 words.

### `selfCorrectionPrompt(reviewText, previousResult, score)`
**Technique:** Self-correction / reflective prompting

Receives the previous LLM result as a JSON string and the bad score. Explains the logical
contradiction (`score < 4` + `sentiment == "Positive"`) and asks the model to re-analyze and
return a corrected, consistent result using the same schema.

---

## Kafka Client

`kafka/kafkaClient.ts` exports two factory functions:

```typescript
createProducer(clientId: string): Promise<Producer>
createConsumer(groupId: string, clientId: string): Promise<Consumer>
```

Both read `KAFKA_BROKER` from the environment (default `"localhost:9092"`), create a `Kafka`
instance, connect, and return the connected client. All three services use these helpers
rather than configuring KafkaJS directly.

---

## Logging

Key log lines emitted by the processor, in the order they appear per message:

```
[processor] Started. Waiting for reviews...

[processor] Received review <reviewId>: "<text>"
[processor] Routing decision for <reviewId>: intent="<intent>" reason="<reason>"
[processor] Skipping <reviewId> — not a review.              ← on intent == "ignore"

[processor] Inconsistency detected for <reviewId> (score=<n>, sentiment=Positive). Running self-correction...
[processor] Self-correction complete — score=<n>, sentiment=<sentiment>

[processor] Insight published for <reviewId>

[processor] LLM call failed: <error>                         ← when OpenAI throws
```

---

## Running the System

### Prerequisites
- Docker running
- Bun installed
- `.env` file at the project root containing `OPENAI_API_KEY=...`

### Start order

```bash
# 1. Start Kafka (from exercise3/)
docker compose up -d
bash topics.sh

# 2. Install dependencies
bun install

# 3. Start analytics first (so no insights are missed)
bun run analytics.ts

# 4. Start processor
bun run processor.ts

# 5. Start producer and type reviews
bun run producer.ts
```

Run all services from the **project root** or from `exercise3/` — dotenv looks for `.env` in
the current working directory. If running from `exercise3/`, ensure `.env` is also present
there or symlinked.

### Teardown

```bash
docker compose down -v
```

---

## Design Decisions

### Three separate services (producer / processor / analytics)

Each service has a single responsibility and communicates only through Kafka topics. This means:

- **Separation of concerns** — the producer knows nothing about LLM analysis; the analytics
  consumer knows nothing about how insights are generated. Each can be modified, restarted, or
  replaced independently.
- **Scalable pipeline** — multiple processor instances could consume from `raw-reviews-topic`
  in parallel (same consumer group) to handle higher review volume without changing any other
  service.
- **Replaceable LLM layer** — `llmClient.ts` is the only file that imports the OpenAI SDK.
  Switching to a different model, provider, or prompt strategy requires changes in one file
  only. The Kafka pipeline structure is unchanged.
- **Observable pipeline** — because events flow through named Kafka topics, any stage can be
  inspected independently with `kafka-console-consumer.sh` without touching the services.

### Fire-and-forget producer

The producer does not await acks. This keeps the CLI responsive for rapid review submission
and reflects real-world ingest patterns where durability guarantees are the broker's
responsibility, not the client's.

### Offset committed after publish

The processor commits Kafka offsets only after the insight has been successfully published to
`processed-insights-topic`. If the publish fails, the message will be re-delivered on restart,
ensuring no review is silently dropped.

### Self-correction as a separate conditional step

Rather than running self-correction on every message, the check (`score < 4 AND sentiment ==
"Positive"`) is evaluated on the analyzer result before deciding whether to make an extra API
call. This keeps the happy path to two LLM calls (router + analyzer) and only adds the third
call when an inconsistency is actually detected.

---

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| `kafkajs` | `^2.2.4` | Kafka producer and consumer client |
| `openai` | `^4.77.0` | OpenAI API SDK |
| `dotenv` | `^16.4.5` | Load `.env` file into `process.env` |
| `bun-types` | `^1.3.10` | TypeScript types for Bun APIs (`Bun.stdin`, etc.) |
| `@types/node` | `^25.3.5` | TypeScript types for Node.js globals |
