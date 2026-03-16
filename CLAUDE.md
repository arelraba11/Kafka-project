# Kafka Beginners Course — Architecture Guide

## 1. Project Overview

This repository is a progressive Kafka learning course that builds a multi-exercise event-driven AI system. Each exercise adds a new layer of complexity on top of the previous one.

The course contains two tracks:

- **Java Track** (`kafka-basics`, `kafka-producer-wikimedia`, `kafka-consumer-opensearch`, `kafka-streams-wikimedia`) — foundational Kafka producer/consumer/streams demos using `org.apache.kafka.clients`.
- **Exercise Track** (`exercise1`, `exercise2`, `exercise3`, `exercise4`) — a progressive TypeScript system (Bun + KafkaJS) building toward an event-driven AI agent. All services live in the top-level `services/` directory and share modules from `shared/`.

---

## 2. Repository Structure

```
kafka-beginners-course-main/
├── .claude/                        # Claude Code workspace (assignments, rules, skills)
├── .env                            # OPENAI_API_KEY, ROUTER_MODE (not committed)
├── .env.example                    # Environment variable reference
├── package.json                    # Bun dependencies: kafkajs, openai
├── tsconfig.json                   # TypeScript config (target: ES2022, strict)
├── build.gradle                    # Root Gradle config
├── settings.gradle                 # Gradle multi-module definitions
│
├── kafka-basics/                   # Java: Producer/Consumer fundamentals
├── kafka-producer-wikimedia/       # Java: Wikimedia SSE → Kafka producer
├── kafka-consumer-opensearch/      # Java: Kafka → OpenSearch bulk consumer
├── kafka-streams-wikimedia/        # Java: Kafka Streams aggregations
│
├── exercise4/
│   └── README.md                   # Exercise 4 architecture notes
│
├── infra/
│   ├── docker-compose.yml          # Single-broker Kafka 3.8.0 (KRaft mode, port 9092)
│   └── topics.sh                   # Creates all Kafka topics for Exercises 1–4
│
├── scripts/
│   ├── start-ex1.sh                # Start Exercise 1 services (background, logs to scripts/logs/ex1-services/)
│   ├── start-ex2.sh                # Start Exercise 2 services (background, logs to scripts/logs/ex2-services/)
│   ├── start-ex3.sh                # Start Exercise 3 services (background, logs to scripts/logs/ex3-services/)
│   ├── start-ex4.sh                # Start Exercise 4 services (background, logs to scripts/logs/ex4-services/)
│   └── stop-all.sh                 # Kill all Bun processes (pkill -f bun)
│
├── shared/                         # Shared modules used by all TypeScript services
│   ├── kafka/
│   │   └── client.ts               # KafkaJS factory: createProducer, createConsumer, sendMessage, subscribeAndRun
│   ├── llm/
│   │   └── openai.ts               # OpenAI wrapper: callLLM(prompt) → string (gpt-4o-mini)
│   ├── prompts/
│   │   ├── prompts.ts              # LLM prompt templates (Exercises 2 and 3)
│   │   └── customerSupportPrompts.ts # LLM prompt templates (Exercise 4)
│   ├── types/
│   │   ├── events.ts               # Exercise 1 + 2 event types
│   │   ├── conversation.ts         # ConversationHistory type
│   │   ├── reviews.ts              # Exercise 3 review types
│   │   └── customerSupport.ts      # Exercise 4 customer support types
│   ├── customerSupport/
│   │   └── benchmark.ts            # Latency computation helpers (computeBenchmark, mergeInsight)
│   └── topics.ts                   # Centralized Kafka topic name constants (all exercises)
│
├── services/                       # All TypeScript microservices
│   ├── user-interface/
│   │   └── userInterface.ts        # CLI stdin/stdout — start MANUALLY
│   ├── memory-service/
│   │   └── memoryService.ts        # Conversation history (persists to history.json)
│   ├── router-service/
│   │   └── routerService.ts        # Regex or LLM router (reads ROUTER_MODE env var)
│   ├── response-aggregator/
│   │   └── responseAggregator.ts   # Formats app results → bot-responses
│   ├── apps/
│   │   ├── mathApp.ts              # Arithmetic parser (no eval)
│   │   ├── weatherApp.ts           # Mock weather for 10 cities
│   │   ├── exchangeApp.ts          # Static currency rates (ILS cross-rates)
│   │   └── generalChatApp.ts       # Keyword-matching fallback chat
│   ├── guardrail-service/
│   │   └── guardrailService.ts     # Regex safety filter (politics, malware keywords)
│   ├── llm-router-service/
│   │   └── llmRouterService.ts     # Few-Shot intent classification (gpt-4o-mini)
│   ├── cot-math-service/
│   │   └── cotMathService.ts       # Chain-of-Thought word problem solver (gpt-4o-mini)
│   ├── review-producer/
│   │   └── reviewProducer.ts       # Reads stdin → raw-reviews-topic
│   ├── review-processor/
│   │   └── reviewProcessor.ts      # 3-step LLM pipeline (gpt-4o-mini)
│   ├── review-analytics/
│   │   └── reviewAnalytics.ts      # Real-time insight display + running avg score
│   ├── customer-support-producer/
│   │   └── customerSupportProducer.ts  # Reads stdin → raw-customer-messages — start MANUALLY
│   ├── sanitizer-service/
│   │   └── sanitizerService.ts     # PII scrubber via Ollama (llama3)
│   ├── sentiment-analyzer/
│   │   └── sentimentAnalyzer.ts    # Sentiment classifier POSITIVE/NEGATIVE (gpt-4o-mini)
│   ├── urgency-classifier/
│   │   └── urgencyClassifier.ts    # Urgency classifier Urgent/Complaint/General (gpt-4o-mini)
│   ├── insight-aggregator/
│   │   └── insightAggregator.ts    # Stream join by message_id + alert rule
│   └── python-workers/             # Alternative Python implementations (HuggingFace)
│       ├── sentiment_worker.py     # distilbert sentiment (manual run, not in start-ex4.sh)
│       ├── urgency_worker.py       # bart-large-mnli urgency (manual run, not in start-ex4.sh)
│       └── requirements.txt        # kafka-python, transformers, torch
│
└── scripts/logs/                   # Created automatically by start-ex*.sh scripts
    ├── ex1-services/
    ├── ex2-services/
    ├── ex3-services/
    └── ex4-services/
```

---

## 3. Shared Modules

### `shared/kafka/client.ts`

Single KafkaJS client factory used by every TypeScript service:

```typescript
const kafka = new Kafka({ clientId: "distributed-bot", brokers: ["localhost:9092"] });
```

| Export | Signature | Purpose |
|---|---|---|
| `createProducer` | `() → Promise<Producer>` | Connect and return producer |
| `createConsumer` | `(groupId: string) → Promise<Consumer>` | Connect and return consumer |
| `sendMessage` | `(producer, topic, key, value)` | JSON-stringify and send |
| `subscribeAndRun` | `(consumer, topics[], handler)` | Subscribe and process (fromBeginning=false) |
| `registerShutdown` | `(resources[])` | SIGINT/SIGTERM graceful disconnect |

### `shared/topics.ts`

All topic names are exported as named constants to avoid magic strings:

| Constant | Topic Name |
|---|---|
| `USER_INPUT` | `user-input-events` |
| `USER_CONTROL` | `user-control-events` |
| `HISTORY_UPDATE` | `conversation-history-update` |
| `INTENT_MATH` | `intent-math` |
| `INTENT_WEATHER` | `intent-weather` |
| `INTENT_EXCHANGE` | `intent-exchange` |
| `INTENT_CHAT` | `intent-general-chat` |
| `APP_RESULTS` | `app-results` |
| `BOT_RESPONSES` | `bot-responses` |
| `ROUTER_DECISION` | `router-decision-events` |
| `GUARDRAIL_VIOLATION` | `guardrail-violation-events` |
| `RAW_REVIEWS` | `raw-reviews-topic` |
| `PROCESSED_INSIGHTS` | `processed-insights-topic` |
| `RAW_CUSTOMER_MESSAGES` | `raw-customer-messages` |
| `SANITIZED_MESSAGES` | `sanitized-messages` |
| `ANALYSIS_SENTIMENT` | `analysis-sentiment` |
| `ANALYSIS_URGENCY` | `analysis-urgency` |

### `shared/llm/openai.ts`

```typescript
export const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
export async function callLLM(prompt: string): Promise<string>
// Model: gpt-4o-mini
// Strips markdown code fences from response before returning
```

### `shared/prompts/prompts.ts`

All LLM prompt templates for Exercises 2 and 3:

| Function | Used By | Technique |
|---|---|---|
| `llmRouterPrompt(userInput)` | `llmRouterService.ts` | Few-Shot classification → `{ intent, parameters, confidence }` |
| `llmExtractionPrompt(intent, input)` | `llmRouterService.ts` | Structured JSON extraction |
| `cotMathPrompt(wordProblem)` | `cotMathService.ts` | Chain-of-Thought → `{ reasoning, expression }` |
| `reviewRouterPrompt(input)` | `reviewProcessor.ts` | Zero-Shot classification → `{ intent, reason }` |
| `reviewAnalyzerPrompt(text)` | `reviewProcessor.ts` | Structured JSON → `{ summary, sentiment, score, aspects[] }` |
| `selfCorrectionPrompt(text, prev, score)` | `reviewProcessor.ts` | Self-correction for score/sentiment inconsistencies |

### `shared/prompts/customerSupportPrompts.ts`

Prompt templates for Exercise 4:

| Function | Used By | Technique |
|---|---|---|
| `buildSanitizePrompt(text)` | `sanitizerService.ts` | Instructs Ollama to replace names→[NAME], phones→[NUMBER] |
| `sentimentPrompt(text)` | `sentimentAnalyzer.ts` | Binary classification → `{ label: POSITIVE\|NEGATIVE, score }` |
| `urgencyPrompt(text)` | `urgencyClassifier.ts` | 3-class classification → `{ label, scores: {Urgent, Complaint, General Inquiry} }` |

### `shared/customerSupport/benchmark.ts`

Latency computation helpers used with `PartialInsight` / `FullInsight` from `shared/types/customerSupport.ts`:

| Export | Purpose |
|---|---|
| `computeBenchmark(partial)` | Returns `{ sanitize_latency_ms, sentiment_latency_ms, urgency_latency_ms, total_latency_ms }` |
| `mergeInsight(partial)` | Merges a completed `PartialInsight` into a `FullInsight` |

---

## 4. Kafka Topics

### Java Modules

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `demo_java` | ProducerDemo | ConsumerDemo | Basic producer/consumer demos |
| `wikimedia.recentchange` | WikimediaChangesProducer | OpenSearchConsumer, WikimediaStreamsApp | Real-time Wikimedia edit stream |

### Exercise 1 + 2 — Chatbot Pipeline

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `user-input-events` | UserInterface | RouterService, MemoryService, GuardrailService (Ex2) | User message broadcast |
| `user-control-events` | UserInterface | MemoryService | Reset conversation history |
| `conversation-history-update` | MemoryService | RouterService | History sync for routing context |
| `router-decision-events` | RouterService (Ex2 path) | LLMRouterService, CotMathService | LLM intent + parameters |
| `intent-math` | RouterService / CotMathService | MathApp | Math calculation request |
| `intent-weather` | RouterService / LLMRouterService | WeatherApp | Weather lookup request |
| `intent-exchange` | RouterService / LLMRouterService | ExchangeApp | Currency exchange request |
| `intent-general-chat` | RouterService / LLMRouterService | GeneralChatApp | Fallback chat request |
| `app-results` | MathApp, WeatherApp, ExchangeApp, GeneralChatApp | ResponseAggregator, MemoryService | App response results |
| `bot-responses` | ResponseAggregator | UserInterface | Final formatted reply |
| `guardrail-violation-events` | GuardrailService | — (audit log only) | Blocked unsafe input |

### Exercise 3 — Review Analysis

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `raw-reviews-topic` | ReviewProducer | ReviewProcessor | Raw review text (UUID keyed) |
| `processed-insights-topic` | ReviewProcessor | ReviewAnalytics | Structured LLM-analyzed insight |

### Exercise 4 — Customer Support Analysis

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `raw-customer-messages` | CustomerSupportProducer | SanitizerService | Raw CLI input (UUID keyed) |
| `sanitized-messages` | SanitizerService | SentimentAnalyzer, UrgencyClassifier | PII-scrubbed text, fan-out to two groups |
| `analysis-sentiment` | SentimentAnalyzer | InsightAggregator | Sentiment label + confidence score |
| `analysis-urgency` | UrgencyClassifier | InsightAggregator | Urgency label + per-class scores |

---

## 5. Services Architecture

### Exercise 1 — Distributed Chatbot (Regex Router)

| Service | File | Consumer Group | Consumes | Produces |
|---|---|---|---|---|
| UserInterface | `user-interface/userInterface.ts` | `ui-service` | `bot-responses` | `user-input-events`, `user-control-events` |
| MemoryService | `memory-service/memoryService.ts` | `memory-service` | `user-input-events`, `app-results`, `user-control-events` | `conversation-history-update` |
| RouterService | `router-service/routerService.ts` | `router-service` | `user-input-events`, `conversation-history-update` | `intent-*`, `router-decision-events` |
| MathApp | `apps/mathApp.ts` | `math-service` | `intent-math` | `app-results` |
| WeatherApp | `apps/weatherApp.ts` | `weather-service` | `intent-weather` | `app-results` |
| ExchangeApp | `apps/exchangeApp.ts` | `exchange-service` | `intent-exchange` | `app-results` |
| GeneralChatApp | `apps/generalChatApp.ts` | `chat-service` | `intent-general-chat` | `app-results` |
| ResponseAggregator | `response-aggregator/responseAggregator.ts` | `response-aggregator` | `app-results` | `bot-responses` |

**RouterService classification logic (ROUTER_MODE=regex):**
- Math: `/[\d]+\s*[+\-*/]\s*[\d]+/`
- Weather: `/\b(weather|temperature|forecast|hot|cold|rain|sunny)\b/i`
- Exchange: `/\b(USD|EUR|ILS|GBP|JPY|CHF|CAD|AUD)\b/i`
- Default: general-chat

**MemoryService** persists conversation turns to `services/memory-service/history.json`.

### Exercise 2 — LLM Prompt Engineering (adds to Exercise 1)

Requires Exercise 1 running with `ROUTER_MODE=llm`. Three additional services intercept the pipeline.

| Service | File | Consumer Group | Consumes | Produces | LLM |
|---|---|---|---|---|---|
| GuardrailService | `guardrail-service/guardrailService.ts` | `guardrail-service` | `user-input-events` | `guardrail-violation-events` | None (regex) |
| LLMRouterService | `llm-router-service/llmRouterService.ts` | `llm-router-service` | `router-decision-events` | `intent-weather`, `intent-exchange`, `intent-general-chat`, `app-results` | gpt-4o-mini |
| CotMathService | `cot-math-service/cotMathService.ts` | `cot-math-service` | `router-decision-events` | `intent-math` | gpt-4o-mini |

**GuardrailService** blocks on keyword lists:
- Politics: `politics`, `election`, `government`, `president`, `war`, …
- Malware: `hack`, `exploit`, `sql injection`, `malware`, `virus`, …

**LLMRouterService** uses Few-Shot prompting with 7 examples to classify into `getWeather | currencyExchange | generalChat` and extract parameters as structured JSON.

**CotMathService** distinguishes pure expressions (`42 * 7` → pass directly to `intent-math`) from word problems (`5 apples minus 2` → LLM Chain-of-Thought → extract expression).

### Exercise 3 — Review Analysis Pipeline (standalone)

| Service | File | Consumer Group | Consumes | Produces | LLM |
|---|---|---|---|---|---|
| ReviewProducer | `review-producer/reviewProducer.ts` | — (producer only) | stdin | `raw-reviews-topic` | None |
| ReviewProcessor | `review-processor/reviewProcessor.ts` | `review-processor-group` | `raw-reviews-topic` | `processed-insights-topic` | gpt-4o-mini |
| ReviewAnalytics | `review-analytics/reviewAnalytics.ts` | `review-analytics-group` | `processed-insights-topic` | — (stdout) | None |

**ReviewProcessor** 3-step pipeline:
1. **Step 1 — Zero-Shot router:** Is this a review or not? (`intent: "analyzeReview" | "ignore"`)
2. **Step 2 — Structured extraction:** Extract `{ summary, overall_sentiment, score (1–10), aspects[] }`
3. **Step 3 — Self-correction:** If `score < 4` and `sentiment == "Positive"`, re-run with correction prompt

**ReviewAnalytics** displays each insight in the terminal and maintains a running average score.

### Exercise 4 — Customer Support Analysis Pipeline (standalone)

Demonstrates stream fan-out, parallel AI inference, and event correlation (stream join).

| Service | File | Consumer Group | Consumes | Produces | LLM/Model |
|---|---|---|---|---|---|
| CustomerSupportProducer | `customer-support-producer/customerSupportProducer.ts` | — (producer only) | stdin | `raw-customer-messages` | None |
| SanitizerService | `sanitizer-service/sanitizerService.ts` | `sanitizer-group` | `raw-customer-messages` | `sanitized-messages` | Ollama (llama3) |
| SentimentAnalyzer | `sentiment-analyzer/sentimentAnalyzer.ts` | `sentiment-group` | `sanitized-messages` | `analysis-sentiment` | gpt-4o-mini |
| UrgencyClassifier | `urgency-classifier/urgencyClassifier.ts` | `urgency-group` | `sanitized-messages` | `analysis-urgency` | gpt-4o-mini |
| InsightAggregator | `insight-aggregator/insightAggregator.ts` | `insight-aggregator-group` | `analysis-sentiment`, `analysis-urgency` | — (stdout) | None |

**Fan-out pattern:** `SentimentAnalyzer` and `UrgencyClassifier` consume the same `sanitized-messages` topic with **different consumer groups** so Kafka delivers each message to both independently.

**Alert rule in InsightAggregator:**
- `sentiment.label === "NEGATIVE"` AND `urgency.label === "Urgent"` → `🚨 STRONG ALERT`
- Otherwise → `[insight] normal log`

**Python alternative workers** (`services/python-workers/`) implement the same sentiment and urgency classification using HuggingFace Transformers (distilbert, bart-large-mnli). These are standalone scripts run manually — they are **not** started by `start-ex4.sh`. They subscribe to `sanitized-messages` using the same consumer group IDs (`sentiment-group`, `urgency-group`) so they can replace the TypeScript workers if a local GPU is available.

---

## 6. Event Flow

### Exercise 1 — Chatbot Pipeline (ROUTER_MODE=regex)

```
stdin
  │
  ▼
UserInterface ──► [user-input-events]
                       │
           ┌───────────┴───────────┐
           ▼                       ▼
     MemoryService           RouterService (regex classify)
    (append user msg)              │
           │               ┌───────┴────────┐──────────┐──────────────┐
    [history-update]  [intent-math]  [intent-weather] [intent-exchange] [intent-general-chat]
           │               │               │               │               │
           ▼               ▼               ▼               ▼               ▼
     RouterService      MathApp       WeatherApp      ExchangeApp   GeneralChatApp
                           └──────────┬──────────┘──────────┘──────────────┘
                                      ▼
                                [app-results]
                                      │
                           ┌──────────┴──────────┐
                           ▼                     ▼
                     MemoryService      ResponseAggregator
                   (append bot msg)           │
                                       [bot-responses]
                                              │
                                              ▼
                                       UserInterface (display)
```

### Exercise 2 — LLM Pipeline (ROUTER_MODE=llm)

```
[user-input-events]
       │
       ├──► GuardrailService ──► [guardrail-violation-events]  (if unsafe, stop here)
       │
       └──► RouterService ──► [router-decision-events]
                                       │
                           ┌───────────┴──────────┐
                           ▼                      ▼
                   LLMRouterService         CotMathService
                  (Few-Shot, gpt-4o-mini)   (CoT, gpt-4o-mini)
                           │                      │
               ┌───────────┤               [intent-math]
               ▼           ▼                      │
     [intent-weather] [intent-exchange]           ▼
     [intent-general-chat]                    MathApp
               │                                  │
               ▼                                  │
       WeatherApp / ExchangeApp /                 │
       GeneralChatApp                             │
               └─────────────────────────────────►│
                                            [app-results]
                                                  │
                                       ResponseAggregator
                                                  │
                                          [bot-responses]
                                                  │
                                           UserInterface
```

### Exercise 3 — Review Analysis Pipeline

```
stdin
  │
  ▼
ReviewProducer ──► [raw-reviews-topic]
                           │
                           ▼
                   ReviewProcessor
                     │
                     ├── Step 1: reviewRouterPrompt → is it a review?
                     │           (if "ignore" → skip)
                     ├── Step 2: reviewAnalyzerPrompt → sentiment, score, aspects
                     └── Step 3: selfCorrectionPrompt → fix inconsistencies (if needed)
                           │
                           ▼
                   [processed-insights-topic]
                           │
                           ▼
                   ReviewAnalytics (display + running avg)
```

### Exercise 4 — Customer Support Analysis Pipeline

```
stdin
  │
  ▼
CustomerSupportProducer ──► [raw-customer-messages]
                                       │
                                       ▼
                               SanitizerService
                               (Ollama llama3, PII scrub)
                                       │
                            [sanitized-messages]  ← fan-out to two consumer groups
                                   │         │
                    ┌──────────────┘         └──────────────┐
                    ▼ (sentiment-group)        ▼ (urgency-group)
            SentimentAnalyzer           UrgencyClassifier
            (gpt-4o-mini)               (gpt-4o-mini)
                    │                          │
          [analysis-sentiment]       [analysis-urgency]
                    │                          │
                    └────────────┬─────────────┘
                                 ▼
                         InsightAggregator
                         (stream join by message_id)
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
          🚨 STRONG ALERT             [insight] normal log
          (NEGATIVE + Urgent)
```

### Java Track — Wikimedia Pipeline

```
Wikimedia SSE Stream ──► WikimediaChangesProducer ──► [wikimedia.recentchange]
                                                              │
                                         ┌────────────────────┤
                                         ▼                    ▼
                                 OpenSearchConsumer    WikimediaStreamsApp
                                 (bulk index)          ├── BotCountStreamBuilder
                                                       ├── EventCountTimeseriesBuilder
                                                       └── WebsiteCountStreamBuilder
```

---

## 7. Running the System

### Prerequisites

- Docker and docker-compose
- [Bun](https://bun.sh/) 1.0+
- Java 17+ and Gradle (for Java track only)
- OpenAI API key (for Exercises 2, 3, and 4)
- [Ollama](https://ollama.com/) with `llama3` pulled (for Exercise 4 sanitizer only)

### Environment Setup

```bash
cp .env.example .env
# Set OPENAI_API_KEY=sk-...
# Set ROUTER_MODE=regex  (Exercise 1) or  ROUTER_MODE=llm  (Exercise 2)
```

### Start Kafka and Create Topics

```bash
# Start Kafka broker (KRaft mode, port 9092)
docker-compose -f infra/docker-compose.yml up -d

# Create all topics for Exercises 1–4
bash infra/topics.sh
```

### Exercise 1 — Distributed Chatbot

```bash
bun install

# Start all pipeline services in the background (logs to scripts/logs/ex1-services/)
bash scripts/start-ex1.sh

# Start the User Interface manually in a separate terminal
bun run services/user-interface/userInterface.ts
```

### Exercise 2 — LLM Prompt Engineering

```bash
bun install

# Requires OPENAI_API_KEY and ROUTER_MODE=llm in .env
bash scripts/start-ex2.sh

# Start the User Interface manually in a separate terminal
bun run services/user-interface/userInterface.ts
```

### Exercise 3 — Review Analysis

```bash
bun install

# Requires OPENAI_API_KEY in .env
# Starts reviewAnalytics and reviewProcessor in the background
bash scripts/start-ex3.sh

# Start the interactive review producer in a separate terminal
bun run services/review-producer/reviewProducer.ts
```

### Exercise 4 — Customer Support Analysis

```bash
bun install

# Requires:
#   OPENAI_API_KEY in .env  (sentiment-analyzer, urgency-classifier)
#   Ollama running locally: ollama serve && ollama pull llama3  (sanitizer-service)

# Starts sanitizer, sentiment-analyzer, urgency-classifier, insight-aggregator
bash scripts/start-ex4.sh

# Start the interactive producer in a separate terminal
bun run services/customer-support-producer/customerSupportProducer.ts

# Optional: run Python workers instead of TypeScript sentiment/urgency services
# cd services/python-workers && python -m venv venv && source venv/bin/activate
# pip install -r requirements.txt
# python sentiment_worker.py   # uses same consumer group: sentiment-group
# python urgency_worker.py     # uses same consumer group: urgency-group
```

### Stop All Services

```bash
bash scripts/stop-all.sh
# Runs: pkill -f bun
```

### Java Track

```bash
docker-compose -f infra/docker-compose.yml up -d

./gradlew :kafka-basics:run
./gradlew :kafka-producer-wikimedia:run
./gradlew :kafka-consumer-opensearch:run
./gradlew :kafka-streams-wikimedia:run
```

---

## 8. Logs and Debugging

All `start-ex*.sh` scripts create their log directory automatically and redirect each service's stdout and stderr to a dedicated file.

```bash
# Watch a specific service log in real time
tail -f scripts/logs/ex1-services/router-service.log
tail -f scripts/logs/ex2-services/llm-router-service.log
tail -f scripts/logs/ex3-services/review-processor.log
tail -f scripts/logs/ex4-services/sentiment-analyzer.log

# Check for errors across all logs
grep -i error scripts/logs/**/*.log
```

| Log File | Service | Exercise |
|---|---|---|
| `scripts/logs/ex1-services/memory-service.log` | MemoryService | 1, 2 |
| `scripts/logs/ex1-services/router-service.log` | RouterService | 1, 2 |
| `scripts/logs/ex1-services/response-aggregator.log` | ResponseAggregator | 1, 2 |
| `scripts/logs/ex1-services/math-app.log` | MathApp | 1, 2 |
| `scripts/logs/ex1-services/weather-app.log` | WeatherApp | 1, 2 |
| `scripts/logs/ex1-services/exchange-app.log` | ExchangeApp | 1, 2 |
| `scripts/logs/ex1-services/general-chat-app.log` | GeneralChatApp | 1, 2 |
| `scripts/logs/ex2-services/guardrail-service.log` | GuardrailService | 2 |
| `scripts/logs/ex2-services/llm-router-service.log` | LLMRouterService | 2 |
| `scripts/logs/ex2-services/cot-math-service.log` | CotMathService | 2 |
| `scripts/logs/ex3-services/review-processor.log` | ReviewProcessor | 3 |
| `scripts/logs/ex3-services/review-analytics.log` | ReviewAnalytics | 3 |
| `scripts/logs/ex4-services/sanitizer-service.log` | SanitizerService | 4 |
| `scripts/logs/ex4-services/sentiment-analyzer.log` | SentimentAnalyzer | 4 |
| `scripts/logs/ex4-services/urgency-classifier.log` | UrgencyClassifier | 4 |
| `scripts/logs/ex4-services/insight-aggregator.log` | InsightAggregator | 4 |

**Conversation history** is persisted to `services/memory-service/history.json`. Send `reset` as a message via the UserInterface to clear it (triggers `user-control-events`).

---

## 9. Development Rules

### Java Track

1. **Java only.** Do not introduce other languages into Java modules.
2. **Package structure.** All classes under `io.conduktor.demos.kafka.*`.
3. **Logging.** Use SLF4J. All Kafka log messages must include topic, partition, and offset.
4. **Kafka client.** Use `org.apache.kafka.clients` library.
5. **Serialization.** Default to `StringSerializer` / `StringDeserializer`.
6. **Gradle.** Do not change the multi-module Gradle structure or modify `build.gradle` unless necessary.
7. **Topics.** Do not rename existing Kafka topics.
8. **New code.** Prefer creating new classes over modifying existing ones.

### Adding a New Java Service

1. Create a new class under `io.conduktor.demos.kafka.<module>`.
2. Use `KafkaProducer<String, String>` or `KafkaConsumer<String, String>`.
3. Configure with `Properties` and `StringSerializer`/`StringDeserializer`.
4. Add `private static final Logger log = LoggerFactory.getLogger(YourClass.class);`
5. Log each record with topic, partition, and offset.

### Adding a New TypeScript Service

1. Create a new file under `services/<service-name>/<serviceName>.ts`.
2. Import the Kafka factory from `shared/kafka/client.ts`.
3. Import topic names from `shared/topics.ts`.
4. Import or extend types from `shared/types/`.
5. Register graceful shutdown with `registerShutdown([producer, consumer])`.
6. Add a log redirect entry to the relevant `scripts/start-ex*.sh`.
