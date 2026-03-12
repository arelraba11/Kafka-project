# Unified Kafka Architecture Plan

**Purpose:** This document is the design plan for unifying exercises 1–4 into a single, coherent
Kafka-based event-driven platform to serve as the technical foundation for the final course project.

**Status: DESIGN PHASE ONLY.**
No code changes have been executed yet. This document captures the architecture decisions,
migration strategy, and structural targets agreed upon before implementation begins.

---

## Table of Contents

1. [Repository Understanding](#1-repository-understanding)
2. [Architecture Goal](#2-architecture-goal)
3. [Target Project Structure](#3-target-project-structure)
4. [Topic Architecture](#4-topic-architecture)
5. [Service Architecture](#5-service-architecture)
6. [Shared Modules](#6-shared-modules)
7. [Migration Plan](#7-migration-plan)
8. [Known Issues to Fix](#8-known-issues-to-fix)
9. [Infrastructure Plan](#9-infrastructure-plan)
10. [Summary of Changes](#10-summary-of-changes)

---

## 1. Repository Understanding

### How Each Exercise Works

**Exercise 1 — Rule-Based Distributed Chatbot**
Eight KafkaJS microservices forming a chatbot with pure regex intent routing. `userInterface` reads
CLI input and produces to `user-input-events` and `user-control-events`. `memoryService` persists
conversation history to `history.json` and broadcasts `conversation-history-update`. `routerService`
classifies intent via regex (math/weather/exchange/chat) and routes to `intent-*` topics. Four domain
apps consume those and produce `app-results`. `responseAggregator` formats results and produces
`bot-responses`. All 8 services share a single well-designed client at
`exercise1/shared/kafka/client.ts`.

**Exercise 2 — LLM-Enhanced Pipeline (mostly stubs)**
Five new services layered on top of Exercise 1's topic model. `guardrailService` detects unsafe
content (stub — always returns null). `llmRouterService` tries LLM classification, falls back to
Exercise 1's regex logic. `llmExtractionService`, `jsonParserService`, and `cotMathService` form a
structured extraction chain. Every `callLLM()` throws immediately — all services run on fallback
paths only. Introduces a second naming convention: snake_case topics alongside Exercise 1's
kebab-case ones.

**Exercise 3 — Review Analysis Pipeline**
Three services using OpenAI `gpt-4o-mini`. `producer.ts` → CLI → `raw-reviews-topic`. `processor.ts`
runs a 3-step pipeline: zero-shot router → structured extraction → self-correction (fires if
`score < 4 AND sentiment == "Positive"`). `analytics.ts` maintains a running average. The only
exercise with a working LLM integration.

**Exercise 4 — Customer Support with ML Workers**
A polyglot pipeline: TypeScript services + Python ML workers. `producer.ts` → `raw-customer-messages`.
`sanitizer.ts` calls Ollama for PII scrubbing → `sanitized-messages`. Two Python workers (distilbert
for sentiment, bart-large-mnli for urgency) consume `sanitized-messages` and produce `analysis-*`
topics. `insight-aggregator.ts` correlates both streams in-memory and fires `STRONG ALERT` on
negative + urgent. `benchmark.ts` is unimplemented (stubs throw).

### Duplicated Components (Exact Paths)

| Component | Duplicate Files |
|---|---|
| Kafka client | `exercise1/shared/kafka/client.ts`, `exercise2/kafka/kafka_consumer.ts` + `kafka_producer.ts`, `exercise3/kafka/kafkaClient.ts`, `exercise4/kafka/kafkaClient.ts` |
| Topic constants | `exercise1/shared/topics.ts`, `exercise2/shared/topics.ts`, `exercise3/shared/topics.ts`, `exercise4/shared/topics.ts` |
| Type definitions | `exercise1/shared/types/events.ts` + `conversation.ts`, `exercise2/shared/types/events.ts`, `exercise3/shared/types.ts`, `exercise4/shared/types.ts` |
| Docker Compose | `exercise2/docker-compose.yml`, `exercise3/docker-compose.yml` (both identical KRaft configs) |
| Topic creation | `exercise2/topics.sh`, `exercise3/topics.sh` |
| CLI producer pattern | `exercise1/services/userInterface/userInterface.ts`, `exercise3/producer.ts`, `exercise4/producer.ts` |
| `node_modules/` | All four exercise directories (committed to git) |

---

## 2. Architecture Goal

### Conceptual Story

The unified system is a **multi-modal customer interaction platform**. A customer message enters
through a CLI gateway. It passes through safety guardrails, PII sanitization, and intelligent intent
routing (regex + LLM). Depending on intent, the message is handled by domain services (math, weather,
currency, chat) or analyzed for sentiment, urgency, and review quality. Results are aggregated,
benchmarked, and returned to the user — forming a complete loop from input to insight.

### Data Flow Diagram

```
                          +-----------------+
                          |   CLI Gateway   |
                          | (user-interface)|
                          +--------+--------+
                                   |
                          chat.user-input
                                   |
               +-------------------+-------------------+
               |                   |                   |
               v                   v                   v
       +-------+-------+   +-------+-------+   +-------+-------+
       |   Guardrail   |   |    Memory     |   |  PII Sanitizer|
       |   Service     |   |   Service     |   |   (Ollama)    |
       +-------+-------+   +-------+-------+   +-------+-------+
               |                   |                   |
  system.guardrail-violations  chat.history-update  support.sanitized
               |                   |                   |
               v           +-------+       +-----------+-----------+
       (back to UI)        |               |                       |
                           v               v                       v
                    +------+------+  +-----+----------+  +--------+-------+
                    | Regex Router|  | Sentiment      |  | Urgency Worker |
                    | (ex1 logic) |  | Worker (Python)|  | (Python)       |
                    +------+------+  +-----+----------+  +--------+-------+
                           |               |                       |
                    chat.intent.*   support.sentiment       support.urgency
                           |               |                       |
                           |         +-----+-----------------------+
                           |         |
                           |         v
                           |  +------+----------+
                           |  | Insight         |
                           |  | Aggregator      |
                           |  +------+----------+
                           |         |
                           |    STRONG ALERT (negative + urgent)
                           |
                        +--+------------------+
                        | LLM Router Service  |
                        +--+------------------+
                           |
                    system.router-decisions
                           |
                    +------+------------------+    +-------------------+
                    | LLM Extraction Service  |--->| JSON Parser Svc   |
                    +-------------------------+    +--------+----------+
                                                            |
                                               system.function-requests
                                                            |
                    +-------+-----------+-----------+-------+
                            |           |           |       |
                            v           v           v       v
                          Math       Weather    Exchange  General Chat
                          App          App        App       App
                            |           |           |       |
                            +-----------+-----------+-------+
                                            |
                                    chat.app-results
                                            |
                                   +--------+--------+
                                   | Response        |
                                   | Aggregator      |
                                   +--------+--------+
                                            |
                                    chat.bot-response
                                            |
                                   +--------+--------+
                                   |   CLI Gateway   |
                                   +-----------------+

  (Separate pipeline, same Kafka cluster)

  review-producer ---> review.raw ---> Review Processor (OpenAI 3-step)
                                              |
                                       review.insights ---> Review Analytics
```

### How Exercises Map to the Unified System

| Exercise | Contribution |
|---|---|
| **Exercise 1** | Core chatbot loop: CLI gateway, memory service, regex router, 4 domain apps, response aggregator |
| **Exercise 2** | LLM intelligence layer: guardrail, LLM router, LLM extraction, JSON parser, CoT math |
| **Exercise 3** | Review analysis branch: review processor with self-correction + analytics dashboard |
| **Exercise 4** | Operations intelligence branch: PII sanitizer, sentiment/urgency ML workers, insight aggregator |

---

## 3. Target Project Structure

### Root Placement Decision

The unified architecture lives **at the repository root**. No subdirectory is created. The existing
repo root becomes the final project root, with the Java Gradle modules and Conduktor infrastructure
remaining in place alongside the new TypeScript project.

### Full Directory Tree

```
kafka-beginners-course-main/              <- repo root = project root
│
├── CLAUDE.md                             <- existing (unchanged)
├── README.md                             <- update to describe unified system
├── package.json                          <- NEW: unified TS package
├── tsconfig.json                         <- NEW: shared Bun-compatible TS config
├── .env.example                          <- NEW: OPENAI_API_KEY, KAFKA_BROKER, OLLAMA_*
├── .gitignore                            <- UPDATE: add node_modules/, .env, history.json
├── bun.lock                              <- existing (replaced by unified install)
│
├── archive/                              <- NEW: original exercises preserved for reference
│   ├── exercise1/                        <- moved from ./exercise1/
│   ├── exercise2/                        <- moved from ./exercise2/
│   ├── exercise3/                        <- moved from ./exercise3/
│   └── exercise4/                        <- moved from ./exercise4/
│
├── shared/                               <- NEW: single source of truth for all services
│   ├── kafka/
│   │   └── client.ts                     # createProducer, createConsumer, sendMessage,
│   │                                     #   subscribeAndRun, registerShutdown
│   ├── topics.ts                         # TOPICS const — all namespaced topic names
│   ├── consumer-groups.ts                # CONSUMER_GROUPS const — all group IDs
│   ├── config.ts                         # Centralized env var loading
│   ├── benchmark.ts                      # computeBenchmark(), mergeInsight() (fixed)
│   ├── types/
│   │   ├── chat.ts                       # UserInputEvent, IntentEvents, AppResultEvent,
│   │   │                                 #   BotResponseEvent, ConversationMessage
│   │   ├── llm-pipeline.ts               # RouterDecisionEvent, LLMResponseEvent,
│   │   │                                 #   FunctionExecutionRequestEvent, GuardrailViolationEvent
│   │   ├── review.ts                     # ReviewEvent, AnalysisResult, ReviewInsightEvent
│   │   ├── support.ts                    # RawMessage, SanitizedMessage, SentimentResult,
│   │   │                                 #   UrgencyResult, PartialInsight, FullInsight
│   │   └── index.ts                      # barrel: export * from all four
│   ├── llm/
│   │   ├── openai-client.ts              # generateText(prompt): Promise<string>
│   │   └── ollama-client.ts              # callOllama(prompt): Promise<string>
│   └── prompts/
│       ├── router-prompts.ts             # llmRouterPrompt(), llmExtractionPrompt()
│       ├── review-prompts.ts             # reviewRouterPrompt(), reviewAnalyzerPrompt(),
│       │                                 #   selfCorrectionPrompt()
│       ├── sanitizer-prompts.ts          # buildSanitizePrompt()
│       └── chat-prompts.ts               # cotMathPrompt(), generalChatPersonaPrompt
│
├── services/                             <- NEW: one folder per microservice
│   ├── user-interface/
│   │   └── user-interface.ts             # CLI gateway (from archive/exercise1)
│   ├── memory-service/
│   │   └── memory-service.ts             # Conversation history (from archive/exercise1)
│   ├── router-service/
│   │   └── regex-router.ts               # Regex intent classifier (from archive/exercise1)
│   ├── guardrail-service/
│   │   └── guardrail-service.ts          # Safety filter with keyword rules (from archive/exercise2)
│   ├── llm-router-service/
│   │   └── llm-router-service.ts         # LLM + regex fallback router (from archive/exercise2)
│   ├── llm-extraction-service/
│   │   └── llm-extraction-service.ts     # Structured param extraction (from archive/exercise2)
│   ├── json-parser-service/
│   │   └── json-parser-service.ts        # LLM output validator (from archive/exercise2)
│   ├── cot-math-service/
│   │   └── cot-math-service.ts           # Chain-of-thought math (from archive/exercise2)
│   ├── math-app/
│   │   └── math-app.ts                   # Arithmetic handler (from archive/exercise1)
│   ├── weather-app/
│   │   └── weather-app.ts                # Weather domain (from archive/exercise1)
│   ├── exchange-app/
│   │   └── exchange-app.ts               # Currency conversion (from archive/exercise1)
│   ├── general-chat-app/
│   │   └── general-chat-app.ts           # Conversational responses (from archive/exercise1)
│   ├── response-aggregator/
│   │   └── response-aggregator.ts        # Result formatter (from archive/exercise1)
│   ├── sanitizer/
│   │   ├── sanitizer.ts                  # PII scrubbing via Ollama (from archive/exercise4)
│   │   └── customer-producer.ts          # CLI for customer messages (from archive/exercise4)
│   ├── review-processor/
│   │   ├── review-processor.ts           # 3-step LLM review pipeline (from archive/exercise3)
│   │   └── review-producer.ts            # CLI for review input (from archive/exercise3)
│   ├── review-analytics/
│   │   └── review-analytics.ts           # Running average tracker (from archive/exercise3)
│   └── insight-aggregator/
│       └── insight-aggregator.ts         # Sentiment+urgency correlation (from archive/exercise4)
│
├── workers/                              <- NEW: Python ML workers
│   ├── sentiment_worker.py               # distilbert sentiment (from archive/exercise4)
│   ├── urgency_worker.py                 # bart-large-mnli urgency (from archive/exercise4)
│   └── requirements.txt                  # kafka-python, transformers, torch
│
├── infra/                                <- NEW: local dev infrastructure
│   ├── docker-compose.yml                # Kafka broker (KRaft mode)
│   ├── topics.sh                         # Create all namespaced topics
│   ├── start-all.sh                      # Launch all background services
│   └── stop-all.sh                       # Graceful shutdown
│
├── conduktor-platform/                   <- EXISTING: keep as-is (optional debug stack)
│   └── docker-compose.yml
│
├── kafka-basics/                         <- EXISTING: Java Gradle module (unchanged)
├── kafka-producer-wikimedia/             <- EXISTING: Java Gradle module (unchanged)
├── kafka-consumer-opensearch/            <- EXISTING: Java Gradle module (unchanged)
├── kafka-streams-wikimedia/              <- EXISTING: Java Gradle module (unchanged)
├── build.gradle                          <- EXISTING: Java root build (unchanged)
└── settings.gradle                       <- EXISTING: Java modules declaration (unchanged)
```

### Coexistence with Java Gradle Modules

The TypeScript project and existing Java Gradle modules coexist at the same root without conflict:

- Java modules are governed by `build.gradle` / `settings.gradle`
- TypeScript project is governed by `package.json` / `tsconfig.json`
- Gradle ignores `services/`, `shared/`, `workers/`, `infra/`
- Bun ignores `kafka-basics/`, `kafka-producer-wikimedia/`, etc.
- The only shared resource is the Kafka broker at `127.0.0.1:9092`

---

## 4. Topic Architecture

### Namespace Design

Topics are organized into four namespaces. The dot separator is the standard Kafka namespacing
convention and is compatible with KafkaJS and the Java `kafka-clients` library.

```
chat.*      — chatbot pipeline (exercises 1 + 2)
review.*    — review analysis pipeline (exercise 3)
support.*   — customer support pipeline (exercise 4)
system.*    — cross-cutting infrastructure (LLM layer, guardrails)
```

This makes topic purpose immediately visible in Kafka UI tools. Topics sort alphabetically by
namespace, making the full topic list self-documenting. New topics can be added to any namespace
without disrupting the others.

### Complete Topic Registry

#### `chat.*` — Chatbot Pipeline

| Topic | Role | Producer | Consumer(s) |
|---|---|---|---|
| `chat.user-input` | Raw user messages from CLI | user-interface | memory-service, regex-router, llm-router, guardrail |
| `chat.user-control` | Control commands (`/reset`) | user-interface | memory-service |
| `chat.history-update` | Conversation history broadcast | memory-service | regex-router |
| `chat.intent.math` | Routed math intents | regex-router | math-app |
| `chat.intent.weather` | Routed weather intents | regex-router | weather-app |
| `chat.intent.exchange` | Routed exchange intents | regex-router | exchange-app |
| `chat.intent.general` | Routed general chat intents | regex-router | general-chat-app |
| `chat.app-results` | Domain app responses | math-app, weather-app, exchange-app, general-chat-app | response-aggregator, memory-service |
| `chat.bot-response` | Final formatted response to user | response-aggregator | user-interface |

#### `system.*` — LLM / Cross-Cutting Infrastructure

| Topic | Role | Producer | Consumer(s) |
|---|---|---|---|
| `system.guardrail-violations` | Safety violations detected | guardrail-service | user-interface |
| `system.router-decisions` | LLM classification output | llm-router-service | llm-extraction-service, cot-math-service |
| `system.llm-prompts` | Structured prompts (observability / replay) | llm-extraction-service | (future LLM gateway) |
| `system.llm-responses` | Raw LLM JSON output | llm-extraction-service | json-parser-service |
| `system.function-requests` | Parsed + validated function calls | json-parser-service | math-app, weather-app, exchange-app, general-chat-app |
| `system.cot-expressions` | Math expressions from chain-of-thought | cot-math-service | math-app |
| `system.bot-output` | LLM pipeline error messages | json-parser-service | response-aggregator |

#### `review.*` — Review Analysis Pipeline

| Topic | Role | Producer | Consumer(s) |
|---|---|---|---|
| `review.raw` | Raw product reviews from CLI | review-producer | review-processor |
| `review.insights` | Analyzed review events | review-processor | review-analytics |

#### `support.*` — Customer Support Pipeline

| Topic | Role | Producer | Consumer(s) |
|---|---|---|---|
| `support.raw` | Raw customer messages from CLI | customer-producer | sanitizer |
| `support.sanitized` | PII-scrubbed messages | sanitizer | sentiment-worker, urgency-worker |
| `support.sentiment` | Sentiment classification results | sentiment-worker (Python) | insight-aggregator |
| `support.urgency` | Urgency classification results | urgency-worker (Python) | insight-aggregator |

### Old → New Topic Mapping

| Old Topic Name | New Topic Name | Exercise |
|---|---|---|
| `user-input-events` / `user_input_events` | `chat.user-input` | ex1, ex2 |
| `user-control-events` | `chat.user-control` | ex1 |
| `conversation-history-update` | `chat.history-update` | ex1 |
| `intent-math` | `chat.intent.math` | ex1 |
| `intent-weather` | `chat.intent.weather` | ex1 |
| `intent-exchange` | `chat.intent.exchange` | ex1 |
| `intent-general-chat` | `chat.intent.general` | ex1 |
| `app-results` | `chat.app-results` | ex1 |
| `bot-responses` | `chat.bot-response` | ex1 |
| `guardrail_violation_events` | `system.guardrail-violations` | ex2 |
| `router_decision_events` | `system.router-decisions` | ex2 |
| `llm_prompt_requests` | `system.llm-prompts` | ex2 |
| `llm_response_events` | `system.llm-responses` | ex2 |
| `function_execution_requests` | `system.function-requests` | ex2 |
| `cot_math_expression_events` | `system.cot-expressions` | ex2 |
| `bot_output_events` | `system.bot-output` | ex2 |
| `raw-reviews-topic` | `review.raw` | ex3 |
| `processed-insights-topic` | `review.insights` | ex3 |
| `raw-customer-messages` | `support.raw` | ex4 |
| `sanitized-messages` | `support.sanitized` | ex4 |
| `analysis-sentiment` | `support.sentiment` | ex4 |
| `analysis-urgency` | `support.urgency` | ex4 |

### `shared/topics.ts` Structure

```typescript
export const TOPICS = {
  CHAT: {
    USER_INPUT:     "chat.user-input",
    USER_CONTROL:   "chat.user-control",
    HISTORY_UPDATE: "chat.history-update",
    INTENT: {
      MATH:     "chat.intent.math",
      WEATHER:  "chat.intent.weather",
      EXCHANGE: "chat.intent.exchange",
      GENERAL:  "chat.intent.general",
    },
    APP_RESULTS:  "chat.app-results",
    BOT_RESPONSE: "chat.bot-response",
  },
  SYSTEM: {
    GUARDRAIL_VIOLATIONS: "system.guardrail-violations",
    ROUTER_DECISIONS:     "system.router-decisions",
    LLM_PROMPTS:          "system.llm-prompts",
    LLM_RESPONSES:        "system.llm-responses",
    FUNCTION_REQUESTS:    "system.function-requests",
    COT_EXPRESSIONS:      "system.cot-expressions",
    BOT_OUTPUT:           "system.bot-output",
  },
  REVIEW: {
    RAW:      "review.raw",
    INSIGHTS: "review.insights",
  },
  SUPPORT: {
    RAW:       "support.raw",
    SANITIZED: "support.sanitized",
    SENTIMENT: "support.sentiment",
    URGENCY:   "support.urgency",
  },
} as const;
```

---

## 5. Service Architecture

| # | Service | Lang | Source | Consumes | Produces | External Deps |
|---|---|---|---|---|---|---|
| 1 | user-interface | TS | archive/exercise1 | `chat.bot-response`, `system.guardrail-violations` | `chat.user-input`, `chat.user-control` | None |
| 2 | memory-service | TS | archive/exercise1 | `chat.user-input`, `chat.app-results`, `chat.user-control` | `chat.history-update` | None |
| 3 | regex-router | TS | archive/exercise1 | `chat.user-input`, `chat.history-update` | `chat.intent.*` (4 topics) | None |
| 4 | guardrail-service | TS | archive/exercise2 | `chat.user-input` | `system.guardrail-violations` | None (keyword rules) |
| 5 | llm-router-service | TS | archive/exercise2 | `chat.user-input` | `system.router-decisions` | OpenAI (fallback to regex) |
| 6 | llm-extraction-service | TS | archive/exercise2 | `system.router-decisions` | `system.llm-responses` | OpenAI |
| 7 | json-parser-service | TS | archive/exercise2 | `system.llm-responses` | `system.function-requests`, `system.bot-output` | None |
| 8 | cot-math-service | TS | archive/exercise2 | `system.router-decisions` | `system.cot-expressions` | OpenAI |
| 9 | math-app | TS | archive/exercise1 | `chat.intent.math`, `system.function-requests`, `system.cot-expressions` | `chat.app-results` | None |
| 10 | weather-app | TS | archive/exercise1 | `chat.intent.weather`, `system.function-requests` | `chat.app-results` | None |
| 11 | exchange-app | TS | archive/exercise1 | `chat.intent.exchange`, `system.function-requests` | `chat.app-results` | None |
| 12 | general-chat-app | TS | archive/exercise1 | `chat.intent.general`, `system.function-requests` | `chat.app-results` | None |
| 13 | response-aggregator | TS | archive/exercise1 | `chat.app-results`, `system.bot-output` | `chat.bot-response` | None |
| 14 | sanitizer | TS | archive/exercise4 | `support.raw` | `support.sanitized` | Ollama |
| 15 | review-processor | TS | archive/exercise3 | `review.raw` | `review.insights` | OpenAI gpt-4o-mini |
| 16 | review-analytics | TS | archive/exercise3 | `review.insights` | (console output) | None |
| 17 | insight-aggregator | TS | archive/exercise4 | `support.sentiment`, `support.urgency` | (console alerts) | None |
| 18 | sentiment-worker | Python | archive/exercise4 | `support.sanitized` | `support.sentiment` | HuggingFace distilbert |
| 19 | urgency-worker | Python | archive/exercise4 | `support.sanitized` | `support.urgency` | HuggingFace bart-large-mnli |

**All 19 services are migrated from existing exercises. No new services are introduced.**

---

## 6. Shared Modules

### `shared/kafka/client.ts`

Merge of Exercise 1's client (most complete API surface) with Exercise 3's parameterized `clientId`
pattern. Exports:

```
createProducer(clientId?: string): Promise<Producer>
createConsumer(groupId: string, clientId?: string): Promise<Consumer>
sendMessage(producer, topic, key, value): Promise<void>     <- JSON-serialized
subscribeAndRun(consumer, topics, handler): Promise<void>   <- with JSON parsing
registerShutdown(resources: Array<Producer | Consumer>): void
```

Exercise 1's combined `registerShutdown` signature (accepts both types) is used over Exercise 2's
consumer-only version.

### `shared/topics.ts`

Nested `TOPICS` constant organized by namespace. Services reference topics as
`TOPICS.CHAT.USER_INPUT`, `TOPICS.SUPPORT.SENTIMENT`, etc. — the namespace is explicit at every
call site. Adding a new topic means adding a key to the appropriate group without touching any
other code.

### `shared/consumer-groups.ts`

All consumer group IDs in one constant. Every service reads its group ID from here rather than
hardcoding strings inline.

### `shared/config.ts`

Centralized environment variable loading. The **only** place in the project that imports
`dotenv/config`. All other modules read from the exported `config` object:

```typescript
import "dotenv/config";

export const config = {
  kafka: {
    broker:   process.env.KAFKA_BROKER    ?? "localhost:9092",
    clientId: process.env.KAFKA_CLIENT_ID ?? "unified-pipeline",
  },
  openai: {
    apiKey: process.env.OPENAI_API_KEY ?? "",
    model:  process.env.OPENAI_MODEL   ?? "gpt-4o-mini",
  },
  ollama: {
    baseUrl: process.env.OLLAMA_BASE_URL ?? "http://localhost:11434",
    model:   process.env.OLLAMA_MODEL    ?? "llama3",
  },
};
```

### `shared/types/`

Four domain-scoped files plus a barrel `index.ts`:

| File | Content | Source |
|---|---|---|
| `chat.ts` | `UserInputEvent`, `AppResultEvent`, `BotResponseEvent`, `ConversationMessage`, intent event types | Merge of archive/exercise1 `events.ts` + `conversation.ts` |
| `llm-pipeline.ts` | `RouterDecisionEvent`, `LLMResponseEvent`, `FunctionExecutionRequestEvent`, `GuardrailViolationEvent` | archive/exercise2 `events.ts` |
| `review.ts` | `ReviewEvent`, `AnalysisResult`, `ReviewInsightEvent`, `Sentiment` (title case) | archive/exercise3 `types.ts` |
| `support.ts` | `RawMessage`, `SanitizedMessage`, `SentimentResult`, `UrgencyResult`, `PartialInsight`, `FullInsight` | archive/exercise4 `types.ts` (with `message_id` fix) |
| `index.ts` | `export *` from all four | — |

Note: `Sentiment` type casing intentionally differs between `review.ts` (title case: `"Positive"`)
and `support.ts` (upper case: `"POSITIVE"`). These reflect different model outputs — LLM vs
HuggingFace — and are kept as-is.

### `shared/llm/`

| File | Exports | Source |
|---|---|---|
| `openai-client.ts` | `generateText(prompt: string): Promise<string>` | archive/exercise3 `llm/llmClient.ts` |
| `ollama-client.ts` | `callOllama(prompt: string): Promise<string>` | extracted from archive/exercise4 `sanitizer.ts` |

### `shared/prompts/`

| File | Exports | Source |
|---|---|---|
| `router-prompts.ts` | `llmRouterPrompt()`, `llmExtractionPrompt()` | archive/exercise2 `prompts/prompts.ts` |
| `review-prompts.ts` | `reviewRouterPrompt()`, `reviewAnalyzerPrompt()`, `selfCorrectionPrompt()` | archive/exercise3 `prompts.ts` |
| `sanitizer-prompts.ts` | `buildSanitizePrompt()` | extracted from archive/exercise4 `sanitizer.ts` |
| `chat-prompts.ts` | `cotMathPrompt()`, `generalChatPersonaPrompt` | archive/exercise2 `prompts/prompts.ts` |

### `shared/benchmark.ts`

The fixed version of `archive/exercise4/shared/benchmark.ts`. Implements:
- `computeBenchmark()` — latency calculations using timestamps from the message pipeline
- `mergeInsight()` — constructs `FullInsight` from `PartialInsight` with computed benchmark

---

## 7. Migration Plan

### Phase 0 — Archive Exercises

**Goal:** Preserve the original exercises before touching anything.

1. Create `archive/` directory at the repo root
2. Move `exercise1/` → `archive/exercise1/`
3. Move `exercise2/` → `archive/exercise2/`
4. Move `exercise3/` → `archive/exercise3/`
5. Move `exercise4/` → `archive/exercise4/`
6. Update root `.gitignore` to exclude `node_modules/` inside archive (e.g. `archive/*/node_modules/`)

All subsequent source references in this migration plan point to `archive/exercise*/`.

---

### Phase 1 — Create Shared Infrastructure

**Goal:** Build `shared/` at the repo root as the foundation that all services will import from.

**Files to create (20):**

| File | Source |
|---|---|
| `package.json` | New — deps: kafkajs, openai, dotenv; devDeps: @types/node, typescript |
| `tsconfig.json` | New — target ES2022, module ESNext (Bun-compatible) |
| `.env.example` | New — all env var templates |
| `shared/config.ts` | New — centralized env loading |
| `shared/kafka/client.ts` | Merge: archive/exercise1 client (primary) + archive/exercise3 parameterized clientId |
| `shared/topics.ts` | New — nested TOPICS const with all namespaced topic names |
| `shared/consumer-groups.ts` | New — all consumer group IDs |
| `shared/types/chat.ts` | Merge: archive/exercise1 `events.ts` + `conversation.ts` |
| `shared/types/llm-pipeline.ts` | From archive/exercise2 `events.ts` |
| `shared/types/review.ts` | From archive/exercise3 `types.ts` |
| `shared/types/support.ts` | From archive/exercise4 `types.ts` (with `message_id` fix applied) |
| `shared/types/index.ts` | Barrel export |
| `shared/llm/openai-client.ts` | From archive/exercise3 `llm/llmClient.ts` |
| `shared/llm/ollama-client.ts` | Extract `callOllama` from archive/exercise4 `sanitizer.ts` |
| `shared/prompts/router-prompts.ts` | From archive/exercise2 `prompts/prompts.ts` |
| `shared/prompts/review-prompts.ts` | From archive/exercise3 `prompts.ts` |
| `shared/prompts/sanitizer-prompts.ts` | Extract `buildSanitizePrompt` from archive/exercise4 `sanitizer.ts` |
| `shared/prompts/chat-prompts.ts` | From archive/exercise2 `prompts/prompts.ts` |
| `shared/benchmark.ts` | From archive/exercise4 `shared/benchmark.ts` (stubs implemented) |

---

### Phase 2 — Migrate Exercise 1 Services

**Goal:** Move 8 chatbot services into `services/`, rewiring imports to `shared/`.

| New Location | Source |
|---|---|
| `services/user-interface/user-interface.ts` | archive/exercise1 `services/userInterface/userInterface.ts` |
| `services/memory-service/memory-service.ts` | archive/exercise1 `services/memoryService/memoryService.ts` |
| `services/router-service/regex-router.ts` | archive/exercise1 `services/routerService/routerService.ts` |
| `services/math-app/math-app.ts` | archive/exercise1 `services/mathApp/mathApp.ts` |
| `services/weather-app/weather-app.ts` | archive/exercise1 `services/weatherApp/weatherApp.ts` |
| `services/exchange-app/exchange-app.ts` | archive/exercise1 `services/exchangeApp/exchangeApp.ts` |
| `services/general-chat-app/general-chat-app.ts` | archive/exercise1 `services/generalChatApp/generalChatApp.ts` |
| `services/response-aggregator/response-aggregator.ts` | archive/exercise1 `services/responseAggregator/responseAggregator.ts` |

**Import changes (all files):**

| Old import | New import |
|---|---|
| `../../shared/kafka/client` | `../../shared/kafka/client` |
| `../../shared/topics` | `../../shared/topics` |
| `../../shared/types/events` | `../../shared/types` |
| `../../shared/types/conversation` | `../../shared/types` |

**Topic constant updates:**

| Old reference | New reference |
|---|---|
| `TOPICS.USER_INPUT` / `TOPICS['user-input-events']` | `TOPICS.CHAT.USER_INPUT` |
| `TOPICS['intent-math']` | `TOPICS.CHAT.INTENT.MATH` |
| `TOPICS['bot-responses']` | `TOPICS.CHAT.BOT_RESPONSE` |
| `TOPICS['app-results']` | `TOPICS.CHAT.APP_RESULTS` |

**Additional changes:**
- `response-aggregator.ts`: add subscription to `TOPICS.SYSTEM.BOT_OUTPUT`
- `user-interface.ts`: add subscription to `TOPICS.SYSTEM.GUARDRAIL_VIOLATIONS`

---

### Phase 3 — Migrate Exercise 2 Services

**Goal:** Move 5 LLM pipeline services; replace all `callLLM()` stubs with the real OpenAI client.

| New Location | Source |
|---|---|
| `services/guardrail-service/guardrail-service.ts` | archive/exercise2 `services/guardrailService.ts` |
| `services/llm-router-service/llm-router-service.ts` | archive/exercise2 `services/llmRouterService.ts` |
| `services/llm-extraction-service/llm-extraction-service.ts` | archive/exercise2 `services/llmExtractionService.ts` |
| `services/json-parser-service/json-parser-service.ts` | archive/exercise2 `services/jsonParserService.ts` |
| `services/cot-math-service/cot-math-service.ts` | archive/exercise2 `services/cotMathService.ts` |

**Import changes (all files):**

| Old import | New import |
|---|---|
| `../kafka/kafka_producer` | `../../shared/kafka/client` |
| `../kafka/kafka_consumer` | `../../shared/kafka/client` |
| `../shared/topics` | `../../shared/topics` |
| `../shared/types/events` | `../../shared/types` |
| `../prompts/prompts` | `../../shared/prompts/router-prompts` or `../../shared/prompts/chat-prompts` |
| `callLLM()` stub | `import { generateText } from "../../shared/llm/openai-client"` |

**Topic constant updates (snake_case → namespaced):**

| Old reference | New reference |
|---|---|
| `TOPICS.USER_INPUT_EVENTS` | `TOPICS.CHAT.USER_INPUT` |
| `TOPICS.ROUTER_DECISION_EVENTS` | `TOPICS.SYSTEM.ROUTER_DECISIONS` |
| `TOPICS.LLM_PROMPT_REQUESTS` | `TOPICS.SYSTEM.LLM_PROMPTS` |
| `TOPICS.LLM_RESPONSE_EVENTS` | `TOPICS.SYSTEM.LLM_RESPONSES` |
| `TOPICS.FUNCTION_EXECUTION_REQUESTS` | `TOPICS.SYSTEM.FUNCTION_REQUESTS` |
| `TOPICS.COT_MATH_EXPRESSION_EVENTS` | `TOPICS.SYSTEM.COT_EXPRESSIONS` |
| `TOPICS.GUARDRAIL_VIOLATION_EVENTS` | `TOPICS.SYSTEM.GUARDRAIL_VIOLATIONS` |
| `TOPICS.BOT_OUTPUT_EVENTS` | `TOPICS.SYSTEM.BOT_OUTPUT` |

---

### Phase 4 — Migrate Exercise 3 Services

**Goal:** Move 3 review pipeline services.

| New Location | Source |
|---|---|
| `services/review-processor/review-processor.ts` | archive/exercise3 `processor.ts` |
| `services/review-processor/review-producer.ts` | archive/exercise3 `producer.ts` |
| `services/review-analytics/review-analytics.ts` | archive/exercise3 `analytics.ts` |

**Import changes:**

| Old import | New import |
|---|---|
| `./llm/llmClient` | `../../shared/llm/openai-client` |
| `./kafka/kafkaClient` | `../../shared/kafka/client` |
| `./shared/topics` | `../../shared/topics` |
| `./shared/types` | `../../shared/types` |
| `./prompts` | `../../shared/prompts/review-prompts` |

**Topic constant updates:**

| Old reference | New reference |
|---|---|
| `TOPICS.RAW_REVIEWS` (`"raw-reviews-topic"`) | `TOPICS.REVIEW.RAW` |
| `TOPICS.PROCESSED_INSIGHTS` (`"processed-insights-topic"`) | `TOPICS.REVIEW.INSIGHTS` |

**Additional change:** Add `registerShutdown` — currently absent in `processor.ts`.

---

### Phase 5 — Migrate Exercise 4 Services + Python Workers

**Goal:** Move 3 TypeScript services and 2 Python workers.

| New Location | Source |
|---|---|
| `services/sanitizer/sanitizer.ts` | archive/exercise4 `sanitizer.ts` |
| `services/sanitizer/customer-producer.ts` | archive/exercise4 `producer.ts` |
| `services/insight-aggregator/insight-aggregator.ts` | archive/exercise4 `insight-aggregator.ts` |
| `workers/sentiment_worker.py` | archive/exercise4 `python-workers/sentiment_worker.py` |
| `workers/urgency_worker.py` | archive/exercise4 `python-workers/urgency_worker.py` |
| `workers/requirements.txt` | archive/exercise4 `python-workers/requirements.txt` |

**TypeScript import changes:**

| Old import | New import |
|---|---|
| `./kafka/kafkaClient` | `../../shared/kafka/client` |
| `./shared/topics` | `../../shared/topics` |
| `./shared/types` | `../../shared/types` |
| Inline `callOllama()` in sanitizer | `../../shared/llm/ollama-client` |
| Inline `buildSanitizePrompt()` in sanitizer | `../../shared/prompts/sanitizer-prompts` |

**Topic constant updates:**

| Old reference | New reference |
|---|---|
| `TOPICS.RAW_CUSTOMER_MESSAGES` | `TOPICS.SUPPORT.RAW` |
| `TOPICS.SANITIZED_MESSAGES` | `TOPICS.SUPPORT.SANITIZED` |
| `TOPICS.ANALYSIS_SENTIMENT` | `TOPICS.SUPPORT.SENTIMENT` |
| `TOPICS.ANALYSIS_URGENCY` | `TOPICS.SUPPORT.URGENCY` |

**Python worker changes:**
- Update `TOPIC_IN` / `TOPIC_OUT` to `support.sanitized`, `support.sentiment`, `support.urgency`
- Change output payload field from `"id"` → `"message_id"` in both workers

**Additional fixes:**
- `insight-aggregator.ts`: update local interfaces to use `message_id` instead of `id`
- Add `registerShutdown` to `sanitizer.ts` and `insight-aggregator.ts`

---

### Phase 6 — Fix Known Bugs

| # | File | Fix |
|---|---|---|
| 1 | `shared/types/support.ts` | Standardize on `message_id` (resolves cascade across Python workers + aggregator) |
| 2 | `shared/benchmark.ts` | Implement `computeBenchmark()` and `mergeInsight()` using TODO formulas |
| 3 | `services/guardrail-service/guardrail-service.ts` | Implement keyword-based `detectViolation()` |
| 4 | `services/llm-router-service/llm-router-service.ts` | Replace `callLLM()` stub with `generateText` |
| 5 | `services/llm-extraction-service/llm-extraction-service.ts` | Same |
| 6 | `services/cot-math-service/cot-math-service.ts` | Same |

---

### Phase 7 — Integration Testing and Cleanup

1. Create `infra/docker-compose.yml`, `infra/topics.sh`, `infra/start-all.sh`, `infra/stop-all.sh`
2. Run `bun install` at repo root (single `node_modules/`)
3. Run `pip install -r workers/requirements.txt`
4. Start Kafka: `docker compose -f infra/docker-compose.yml up -d`
5. Create topics: `bash infra/topics.sh`
6. Verify four end-to-end flows:
   - **Chatbot:** `"What is 2+3?"` → math-app → `chat.app-results` → `chat.bot-response` → display
   - **LLM Router:** `"Weather in Paris?"` → llm-router → `system.router-decisions` → weather-app
   - **Review:** product review text → `review.raw` → processor → `review.insights` → analytics output
   - **Customer Support:** `"Jane called 555-1234 and she's furious"` → PII scrubbed → NEGATIVE + Urgent → STRONG ALERT
7. Update root `README.md` with unified documentation

---

## 8. Known Issues to Fix

| # | Issue | Location | Fix |
|---|---|---|---|
| **1** | `id` vs `message_id` field mismatch | `archive/exercise4/insight-aggregator.ts` local interfaces use `id`; `archive/exercise4/shared/types.ts` defines `message_id`; Python workers also emit `"id"` | Standardize on `message_id` in `shared/types/support.ts`, sanitizer output, both Python worker payloads, and insight-aggregator local interfaces |
| **2** | `benchmark.ts` stubs | `archive/exercise4/shared/benchmark.ts` — `computeBenchmark()` returns `null`; `mergeInsight()` throws `"not implemented"` | Implement using the TODO formulas: `sanitize_latency = t_sanitizer_out - t_produced`, `total_latency = max(t_sentiment_out, t_urgency_out) - t_produced` |
| **3** | Guardrail always null | `archive/exercise2/services/guardrailService.ts:14` | Implement keyword detection for political and malware terms |
| **4** | Exercise 2 LLM stubs throw | `llmRouterService.ts:18`, `llmExtractionService.ts:14`, `cotMathService.ts:15` | Replace all three with `import { generateText } from "../../shared/llm/openai-client"` |
| **5** | Mixed topic naming conventions | Exercise 1 kebab-case vs Exercise 2 snake_case | All topics standardized to dot-namespaced kebab-case in `shared/topics.ts` |
| **6** | Duplicate `USER_INPUT` key in exercise 2 | `archive/exercise2/shared/topics.ts` has both `USER_INPUT: "user-input-events"` and `USER_INPUT_EVENTS: "user_input_events"` | Unified to single `TOPICS.CHAT.USER_INPUT` |
| **7** | `node_modules` committed to git | All four exercise directories | Add `archive/*/node_modules/` to `.gitignore`; single root install |
| **8** | Missing `registerShutdown` | `archive/exercise4/sanitizer.ts`, `archive/exercise4/insight-aggregator.ts`, `archive/exercise3/processor.ts` | Add `registerShutdown([consumer, producer])` during migration |

---

## 9. Infrastructure Plan

### Two-Layer Infrastructure Strategy

| Layer | Location | Purpose | When to Use |
|---|---|---|---|
| **Kafka broker** | `infra/docker-compose.yml` | Minimal KRaft-mode Kafka | Day-to-day development |
| **Conduktor platform** | `conduktor-platform/docker-compose.yml` | Full Conduktor stack with Console UI | Debugging, topic inspection, consumer group monitoring |

These run on the same port (9092) and should **not** run simultaneously. The service broker address
(`localhost:9092`) is identical for both, so no service code changes when switching stacks.

### `infra/docker-compose.yml`

```yaml
version: "3.8"
services:
  kafka:
    image: apache/kafka:latest
    container_name: kafka
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: broker,controller
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      ALLOW_PLAINTEXT_LISTENER: "yes"
    ports:
      - "9092:9092"
    volumes:
      - kafka_data:/bitnami/kafka
volumes:
  kafka_data:
```

Ollama runs natively on the host (not dockerized — requires GPU access).

### `infra/topics.sh`

```bash
#!/bin/bash
set -e
KAFKA_CONTAINER="kafka"
KAFKA_BIN="/opt/kafka/bin/kafka-topics.sh"
BOOTSTRAP="localhost:9092"

TOPICS=(
  # chat pipeline
  "chat.user-input"
  "chat.user-control"
  "chat.history-update"
  "chat.intent.math"
  "chat.intent.weather"
  "chat.intent.exchange"
  "chat.intent.general"
  "chat.app-results"
  "chat.bot-response"
  # system / LLM layer
  "system.guardrail-violations"
  "system.router-decisions"
  "system.llm-prompts"
  "system.llm-responses"
  "system.function-requests"
  "system.cot-expressions"
  "system.bot-output"
  # review pipeline
  "review.raw"
  "review.insights"
  # support pipeline
  "support.raw"
  "support.sanitized"
  "support.sentiment"
  "support.urgency"
  # Java demo topics (existing Gradle modules)
  "demo_java"
  "wikimedia.recentchange"
)

for TOPIC in "${TOPICS[@]}"; do
  docker exec "$KAFKA_CONTAINER" "$KAFKA_BIN" \
    --bootstrap-server "$BOOTSTRAP" \
    --create --topic "$TOPIC" \
    --partitions 3 --replication-factor 1 --if-not-exists
  echo "Created: $TOPIC"
done
```

### `package.json` Scripts

```json
{
  "scripts": {
    "infra:up":          "docker compose -f infra/docker-compose.yml up -d",
    "infra:down":        "docker compose -f infra/docker-compose.yml down",
    "infra:topics":      "bash infra/topics.sh",
    "infra:conduktor":   "docker compose -f conduktor-platform/docker-compose.yml up -d",
    "setup":             "bun install && pip install -r workers/requirements.txt",

    "start:ui":                "bun run services/user-interface/user-interface.ts",
    "start:memory":            "bun run services/memory-service/memory-service.ts",
    "start:regex-router":      "bun run services/router-service/regex-router.ts",
    "start:guardrail":         "bun run services/guardrail-service/guardrail-service.ts",
    "start:llm-router":        "bun run services/llm-router-service/llm-router-service.ts",
    "start:llm-extraction":    "bun run services/llm-extraction-service/llm-extraction-service.ts",
    "start:json-parser":       "bun run services/json-parser-service/json-parser-service.ts",
    "start:cot-math":          "bun run services/cot-math-service/cot-math-service.ts",
    "start:math":              "bun run services/math-app/math-app.ts",
    "start:weather":           "bun run services/weather-app/weather-app.ts",
    "start:exchange":          "bun run services/exchange-app/exchange-app.ts",
    "start:chat":              "bun run services/general-chat-app/general-chat-app.ts",
    "start:aggregator":        "bun run services/response-aggregator/response-aggregator.ts",
    "start:sanitizer":         "bun run services/sanitizer/sanitizer.ts",
    "start:review-processor":  "bun run services/review-processor/review-processor.ts",
    "start:review-analytics":  "bun run services/review-analytics/review-analytics.ts",
    "start:insight-aggregator":"bun run services/insight-aggregator/insight-aggregator.ts",
    "start:review-producer":   "bun run services/review-processor/review-producer.ts",
    "start:customer-producer": "bun run services/sanitizer/customer-producer.ts",
    "start:workers":           "python workers/sentiment_worker.py & python workers/urgency_worker.py &"
  }
}
```

### `conduktor-platform/` Role

The existing `conduktor-platform/docker-compose.yml` is kept **exactly as-is**. When running the
Conduktor stack, all services connect to the same `localhost:9092` — no service configuration
changes. The namespaced topic structure (`chat.*`, `review.*`, `support.*`, `system.*`) is
particularly useful in the Conduktor Console because topics sort alphabetically and namespace
grouping makes the topic list self-documenting.

---

## 10. Summary of Changes

### What This Migration Achieves

| Before | After |
|---|---|
| 4 independent projects in `exercise*/` | 1 unified project at the repo root |
| 4 Kafka client implementations | 1 shared client in `shared/kafka/client.ts` |
| 4 topic files with conflicting conventions | 1 `shared/topics.ts` with namespaced topics |
| 4 sets of type definitions | 4 domain-scoped files under `shared/types/` |
| 2 docker-compose files + 1 missing | 1 `infra/docker-compose.yml` + Conduktor available |
| LLM stubs that throw immediately | Real OpenAI client wired through `shared/llm/` |
| `id`/`message_id` mismatch across 4 files | Standardized on `message_id` everywhere |
| `node_modules/` committed 4× | Single root install, not committed |
| No graceful shutdown in 3 services | `registerShutdown` added uniformly |
| Guardrail always returns null | Basic keyword detection implemented |
| Benchmark stubs throw | Latency formulas implemented |
| Original exercises deleted | Original exercises preserved in `archive/` |

### What Does NOT Change

- No existing functionality is removed
- All 19 services are migrated from existing exercises — no new services created
- The Java Gradle modules (`kafka-basics`, `kafka-producer-wikimedia`, etc.) are untouched
- The Conduktor platform directory is untouched
- The migration is structural (imports, topic names, folder layout) — not functional
