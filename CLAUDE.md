# Kafka Beginners Course — Architecture Guide

## 1. Project Overview

This repository is a progressive Kafka learning course that builds a multi-exercise event-driven AI system. Each exercise adds a new layer of complexity on top of the previous one, culminating in a production-style streaming AI pipeline.

The course contains two tracks:

- **Java Track** (`kafka-basics`, `kafka-producer-wikimedia`, `kafka-consumer-opensearch`, `kafka-streams-wikimedia`) — foundational Kafka producer/consumer/streams demos using the official `org.apache.kafka.clients` library.
- **Exercise Track** (`exercise1` through `exercise4`) — a progressive system using TypeScript (Bun + KafkaJS) and Python workers, building toward an event-driven AI agent.

---

## 2. System Architecture

The system follows an **event-driven microservices architecture** where every service communicates exclusively through Kafka topics. No direct service-to-service calls are made.

```
User Input
    │
    ▼
[UserInterface / Producer]
    │  user-input-events
    ▼
[RouterService / GuardrailService]
    │  intent-* topics / router_decision_events
    ▼
[Domain Apps / LLM Workers]
    │  app-results / llm_response_events / analysis-*
    ▼
[Aggregator / ResponseAggregator]
    │  bot-responses / sanitized-messages
    ▼
[UserInterface / Analytics / Insight Aggregator]
```

### Core Patterns Used

- **Fan-out**: One topic consumed by multiple consumer groups (Exercise 4: `sanitized-messages` → sentiment + urgency workers)
- **Stream Join**: Correlating events by message ID across multiple topics (Exercise 4: aggregator joins sentiment + urgency)
- **LLM-as-router**: Using language models for intent classification instead of regex (Exercise 2)
- **Self-Correcting Pipeline**: Multi-step LLM chain with validation pass (Exercise 3)
- **Parallel Inference**: Multiple ML workers consuming the same topic in parallel (Exercise 4)

---

## 3. Repository Structure

```
kafka-beginners-course-main/
├── .claude/                        # Claude Code workspace
│   ├── assignments/                # Exercise task definitions
│   ├── rules/                      # Code style and project rules
│   └── skills/                     # Kafka skill templates
│
├── kafka-basics/                   # Java: Producer/Consumer fundamentals
├── kafka-producer-wikimedia/       # Java: Wikimedia → Kafka producer
├── kafka-consumer-opensearch/      # Java: Kafka → OpenSearch consumer
├── kafka-streams-wikimedia/        # Java: Kafka Streams aggregations
│
├── exercise1/                      # Distributed chatbot (8 microservices)
│   ├── shared/                     # Kafka client, topics, shared types
│   └── services/                   # userInterface, memoryService, routerService, apps, aggregator
│
├── exercise2/                      # LLM prompt engineering pipeline
│   ├── shared/                     # Topics, event types
│   ├── kafka/                      # Producer/consumer utilities
│   ├── prompts/                    # LLM prompt templates
│   └── services/                   # guardrail, llmRouter, llmExtraction, jsonParser, cotMath
│
├── exercise3/                      # Real-time review analysis
│   ├── shared/                     # Topics, types
│   ├── kafka/                      # KafkaJS client factory
│   ├── llm/                        # OpenAI client wrapper
│   ├── producer.ts                 # Review producer
│   ├── processor.ts                # 3-step LLM pipeline
│   ├── analytics.ts                # Real-time insight consumer
│   └── prompts.ts                  # LLM prompt templates
│
├── exercise4/                      # Streaming AI customer support analyzer
│   ├── shared/                     # Topics, types, benchmark utilities
│   ├── kafka/                      # KafkaJS client factory
│   ├── python-workers/             # HuggingFace ML inference workers
│   ├── producer.ts                 # Customer message producer
│   ├── sanitizer.ts                # PII scrubber (Ollama)
│   ├── insight-aggregator.ts       # Stream joiner + alerting
│   └── prompts.ts                  # Ollama prompt for sanitization
│
├── conduktor-platform/             # Conduktor platform docker setup
├── infra/logs/                     # Service execution logs
├── services/chat/                  # Persisted conversation history
├── .env                            # OPENAI_API_KEY, ROUTER_MODE
├── .env.example                    # Environment variable reference
├── build.gradle                    # Root Gradle config
└── settings.gradle                 # Gradle multi-module definitions
```

---

## 4. Kafka Topics

### Java Modules

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `demo_java` | ProducerDemo, ProducerDemoKeys, ProducerDemoWithCallback | ConsumerDemo, ConsumerDemoWithShutdown, ConsumerDemoCooperative | Basic producer/consumer demos |
| `wikimedia.recentchange` | WikimediaChangesProducer | OpenSearchConsumer, WikimediaStreamsApp | Real-time Wikimedia edit stream |

### Exercise 1 — Distributed Chatbot

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `user-input-events` | UserInterface | RouterService, MemoryService | User message broadcast |
| `user-control-events` | UserInterface | MemoryService | Clear history command |
| `conversation-history-update` | MemoryService | RouterService | Context sync for routing decisions |
| `intent-math` | RouterService | MathApp | Math calculation requests |
| `intent-weather` | RouterService | WeatherApp | Weather lookup requests |
| `intent-exchange` | RouterService | ExchangeApp | Currency exchange requests |
| `intent-general-chat` | RouterService | GeneralChatApp | Fallback chat requests |
| `app-results` | MathApp, WeatherApp, ExchangeApp, GeneralChatApp | ResponseAggregator, MemoryService | App responses |
| `bot-responses` | ResponseAggregator | UserInterface | Final formatted reply to user |

### Exercise 2 — LLM Prompt Engineering

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `user_input_events` | External source | GuardrailService, LLMRouterService | Raw user input |
| `router_decision_events` | LLMRouterService | LLMExtractionService, CotMathService | Classified intent with confidence score |
| `llm_response_events` | LLMExtractionService | JSONParserService | Raw LLM structured output |
| `function_execution_requests` | JSONParserService | Domain apps (Exercise 1) | Typed, validated function calls |
| `guardrail_violation_events` | GuardrailService | — | Unsafe input audit log |
| `cot_math_expression_events` | CotMathService | MathApp | Chain-of-thought math expressions |
| `bot_output_events` | Domain apps | ResponseAggregator | App results |

### Exercise 3 — Review Analysis

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `raw-reviews-topic` | producer.ts | processor.ts | Raw review text |
| `processed-insights-topic` | processor.ts | analytics.ts | Structured LLM-analyzed insights |

### Exercise 4 — Customer Support Analyzer

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `raw-customer-messages` | producer.ts | sanitizer.ts | Raw customer support messages |
| `sanitized-messages` | sanitizer.ts | sentiment_worker.py (`sentiment-group`), urgency_worker.py (`urgency-group`) | PII-scrubbed messages (fan-out) |
| `analysis-sentiment` | sentiment_worker.py | insight-aggregator.ts | Sentiment classification result |
| `analysis-urgency` | urgency_worker.py | insight-aggregator.ts | Urgency classification result |

---

## 5. Microservices

### Java Track

| Service | Location | Consumes | Produces | Responsibility |
|---|---|---|---|---|
| ProducerDemo | `kafka-basics` | — | `demo_java` | Basic producer demo |
| ConsumerDemo | `kafka-basics` | `demo_java` | — | Basic consumer demo |
| WikimediaChangesProducer | `kafka-producer-wikimedia` | Wikimedia SSE stream | `wikimedia.recentchange` | Streams Wikipedia edits into Kafka |
| OpenSearchConsumer | `kafka-consumer-opensearch` | `wikimedia.recentchange` | — | Indexes events in OpenSearch via bulk API |
| WikimediaStreamsApp | `kafka-streams-wikimedia` | `wikimedia.recentchange` | — | Aggregates bot counts, event counts, site counts |

### Exercise 1 — TypeScript (Bun + KafkaJS)

| Service | Location | Consumes | Produces | Responsibility |
|---|---|---|---|---|
| UserInterface | `exercise1/services/userInterface` | `bot-responses` | `user-input-events`, `user-control-events` | CLI input/output |
| MemoryService | `exercise1/services/memoryService` | `user-input-events`, `app-results`, `user-control-events` | `conversation-history-update` | Persists conversation history |
| RouterService | `exercise1/services/routerService` | `user-input-events`, `conversation-history-update` | `intent-math`, `intent-weather`, `intent-exchange`, `intent-general-chat` | Intent classification via regex |
| MathApp | `exercise1/services/mathApp` | `intent-math` | `app-results` | Arithmetic expression evaluator |
| WeatherApp | `exercise1/services/weatherApp` | `intent-weather` | `app-results` | Mock weather data lookup |
| ExchangeApp | `exercise1/services/exchangeApp` | `intent-exchange` | `app-results` | Currency exchange via ILS cross-rates |
| GeneralChatApp | `exercise1/services/generalChatApp` | `intent-general-chat` | `app-results` | Rule-based keyword chat |
| ResponseAggregator | `exercise1/services/responseAggregator` | `app-results` | `bot-responses` | Formats and forwards bot reply |

### Exercise 2 — TypeScript (Bun + KafkaJS)

| Service | Location | Consumes | Produces | Responsibility |
|---|---|---|---|---|
| GuardrailService | `exercise2/services/guardrailService.ts` | `user_input_events` | `guardrail_violation_events` | Blocks unsafe content via LLM |
| LLMRouterService | `exercise2/services/llmRouterService.ts` | `user_input_events` | `router_decision_events` | Few-Shot intent classification |
| LLMExtractionService | `exercise2/services/llmExtractionService.ts` | `router_decision_events` | `llm_response_events` | Structured JSON parameter extraction |
| JSONParserService | `exercise2/services/jsonParserService.ts` | `llm_response_events` | `function_execution_requests` | Parses and validates LLM JSON output |
| CotMathService | `exercise2/services/cotMathService.ts` | `router_decision_events` | `cot_math_expression_events` | Chain-of-Thought word problem solver |

### Exercise 3 — TypeScript (Bun + KafkaJS)

| Service | Location | Consumes | Produces | Responsibility |
|---|---|---|---|---|
| producer | `exercise3/producer.ts` | stdin | `raw-reviews-topic` | CLI review ingestion |
| processor | `exercise3/processor.ts` | `raw-reviews-topic` | `processed-insights-topic` | 3-step LLM pipeline (classify → extract → self-correct) |
| analytics | `exercise3/analytics.ts` | `processed-insights-topic` | — | Real-time insight display |

### Exercise 4 — TypeScript + Python

| Service | Location | Consumes | Produces | Responsibility |
|---|---|---|---|---|
| producer | `exercise4/producer.ts` | stdin | `raw-customer-messages` | CLI customer message ingestion |
| sanitizer | `exercise4/sanitizer.ts` | `raw-customer-messages` | `sanitized-messages` | PII scrubbing via Ollama |
| sentiment_worker | `exercise4/python-workers/sentiment_worker.py` | `sanitized-messages` | `analysis-sentiment` | HuggingFace sentiment classification |
| urgency_worker | `exercise4/python-workers/urgency_worker.py` | `sanitized-messages` | `analysis-urgency` | HuggingFace zero-shot urgency detection |
| insight-aggregator | `exercise4/insight-aggregator.ts` | `analysis-sentiment`, `analysis-urgency` | — | Joins sentiment + urgency, fires alerts |

---

## 6. Event Flow

### Exercise 1 — Chatbot Pipeline

```
stdin → UserInterface
           │── user-input-events ──► RouterService ──► intent-math ──► MathApp ──┐
           │                     └──► MemoryService                               │
           │                              │                                        │
           │                    conversation-history-update                        │
           │                              ▼                                        ▼
           │                         RouterService           WeatherApp ──────► app-results
           │                                                 ExchangeApp ─────►    │
           │                                              GeneralChatApp ────►    │
           │                                                                       ▼
           │                                                            ResponseAggregator
           │                                                                       │
           └◄────────────────── bot-responses ◄────────────────────────────────────┘
```

### Exercise 2 — LLM Prompt Engineering Pipeline

```
user_input_events
    ├──► GuardrailService ──► guardrail_violation_events (if unsafe)
    └──► LLMRouterService ──► router_decision_events
                                  ├──► LLMExtractionService ──► llm_response_events
                                  │         └──► JSONParserService ──► function_execution_requests
                                  └──► CotMathService ──► cot_math_expression_events
```

### Exercise 3 — Review Analysis Pipeline

```
stdin → producer ──► raw-reviews-topic ──► processor (3-step LLM)
                                               │ [Step 1: Is it a review? Zero-Shot]
                                               │ [Step 2: Extract summary/sentiment/score]
                                               │ [Step 3: Self-correct inconsistencies]
                                               ▼
                                    processed-insights-topic ──► analytics (display)
```

### Exercise 4 — Customer Support Fan-Out Pipeline

```
stdin → producer ──► raw-customer-messages ──► sanitizer (Ollama PII scrub)
                                                     │
                                           sanitized-messages
                                           ├──► sentiment_worker.py ──► analysis-sentiment ──┐
                                           └──► urgency_worker.py ──► analysis-urgency ──────┤
                                                                                              ▼
                                                                              insight-aggregator (stream join)
                                                                              [NEGATIVE + Urgent → STRONG ALERT]
```

### Java Track — Wikimedia Pipeline

```
Wikimedia SSE Stream ──► WikimediaChangesProducer ──► wikimedia.recentchange
                                                            ├──► OpenSearchConsumer ──► OpenSearch index
                                                            └──► WikimediaStreamsApp
                                                                      ├── BotCountStreamBuilder
                                                                      ├── EventCountTimeseriesBuilder
                                                                      └── WebsiteCountStreamBuilder
```

---

## 7. Development Rules

These rules apply to the **Java track** (`kafka-basics`, `kafka-producer-wikimedia`, `kafka-consumer-opensearch`, `kafka-streams-wikimedia`):

1. **Java only.** Do not introduce Python, Node.js, or any other language into the Java modules.
2. **Package structure.** All Java classes must be under `io.conduktor.demos.kafka.*`.
3. **Logging.** Use SLF4J. All Kafka log messages must include topic, partition, and offset.
4. **Kafka client.** Use `org.apache.kafka.clients` library.
5. **Serialization.** Default to `StringSerializer` / `StringDeserializer`. Use explicit serializers for other types.
6. **Gradle structure.** Do not change the multi-module Gradle structure. Do not modify `build.gradle` unless necessary.
7. **Topic names.** Do not rename existing Kafka topics.
8. **New code.** Prefer creating new classes over modifying existing ones.

### Adding a New Service (Java)

1. Create a new class under `io.conduktor.demos.kafka.<module>`.
2. Use the standard `KafkaProducer<String, String>` or `KafkaConsumer<String, String>` pattern.
3. Configure with `Properties` and `StringSerializer`/`StringDeserializer`.
4. Add SLF4J logger: `private static final Logger log = LoggerFactory.getLogger(YourClass.class);`
5. Log each produced/consumed record including topic, partition, and offset.

### Adding a New Exercise Service (TypeScript)

1. Create a new directory under the relevant exercise (e.g., `exercise4/services/newService/`).
2. Import Kafka client from the exercise's `kafka/kafkaClient.ts` shared module.
3. Import topic names from `shared/topics.ts`.
4. Import or extend types from `shared/types.ts`.
5. Use the KafkaJS consumer/producer pattern established in the exercise.

---

## 8. Running the System

### Prerequisites

- Docker and docker-compose
- [Bun](https://bun.sh/) 1.0+
- Java 17+ and Gradle (for Java track)
- Python 3.10+ (for Exercise 4 Python workers)
- [Ollama](https://ollama.com/) running locally (for Exercise 4 sanitizer)
- OpenAI API key in `.env`

### Environment Setup

```bash
cp .env.example .env
# Fill in OPENAI_API_KEY and configure ROUTER_MODE (regex | llm)
```

### Java Track

```bash
# Start Kafka (using any docker-compose from an exercise)
cd exercise2 && docker-compose up -d

# Run a Java module
./gradlew :kafka-producer-wikimedia:run
./gradlew :kafka-consumer-opensearch:run
./gradlew :kafka-streams-wikimedia:run

# For OpenSearch module, start OpenSearch first
cd kafka-consumer-opensearch && docker-compose up -d
```

### Exercise 1 — Distributed Chatbot

```bash
cd exercise1
bun install

# Start all services (each in a separate terminal)
bun run services/memoryService/memoryService.ts
bun run services/routerService/routerService.ts
bun run services/mathApp/mathApp.ts
bun run services/weatherApp/weatherApp.ts
bun run services/exchangeApp/exchangeApp.ts
bun run services/generalChatApp/generalChatApp.ts
bun run services/responseAggregator/responseAggregator.ts
bun run services/userInterface/userInterface.ts
```

### Exercise 2 — LLM Pipeline

```bash
cd exercise2
docker-compose up -d
bash topics.sh
bun install

# Start services (each in a separate terminal)
bun run services/guardrailService.ts
bun run services/llmRouterService.ts
bun run services/llmExtractionService.ts
bun run services/jsonParserService.ts
bun run services/cotMathService.ts
```

### Exercise 3 — Review Analysis

```bash
cd exercise3
docker-compose up -d
bash topics.sh
bun install

# Terminal 1: analytics consumer
bun run analytics.ts

# Terminal 2: LLM processor
bun run processor.ts

# Terminal 3: producer (sends review lines)
bun run producer.ts
```

### Exercise 4 — Customer Support Analyzer

```bash
cd exercise4
docker-compose up -d   # or reuse an existing Kafka broker
bun install

# Install Python dependencies
pip install -r python-workers/requirements.txt

# Start Ollama with llama3
ollama run llama3

# Terminal 1: sanitizer
bun run sanitizer.ts

# Terminal 2: sentiment worker
python python-workers/sentiment_worker.py

# Terminal 3: urgency worker
python python-workers/urgency_worker.py

# Terminal 4: aggregator
bun run insight-aggregator.ts

# Terminal 5: producer
bun run producer.ts
```

---

## 9. AI Prompt Infrastructure

Prompt templates are defined inline in TypeScript files within each exercise.

| Location | Purpose |
|---|---|
| `exercise2/prompts/prompts.ts` | Few-Shot classification, Structured JSON extraction, Chain-of-Thought math, Persona/Guardrail templates |
| `exercise3/prompts.ts` | Zero-Shot review router, Structured JSON analyzer, Self-correction validator |
| `exercise4/prompts.ts` | Ollama PII sanitization prompt (scrub names and phone numbers) |

### Prompt Engineering Techniques by Exercise

| Exercise | Technique | Where |
|---|---|---|
| Exercise 2 | Few-Shot Prompting | `llmRouterService.ts` → `prompts/prompts.ts` |
| Exercise 2 | Structured JSON Output | `llmExtractionService.ts` → `prompts/prompts.ts` |
| Exercise 2 | Chain-of-Thought (CoT) | `cotMathService.ts` → `prompts/prompts.ts` |
| Exercise 2 | Persona / Guardrails | `guardrailService.ts` → `prompts/prompts.ts` |
| Exercise 3 | Zero-Shot Classification | `processor.ts` → `prompts.ts` (Step 1) |
| Exercise 3 | Structured JSON Output | `processor.ts` → `prompts.ts` (Step 2) |
| Exercise 3 | Self-Correction | `processor.ts` → `prompts.ts` (Step 3) |
| Exercise 4 | PII Sanitization via Ollama | `sanitizer.ts` → `prompts.ts` |

### LLM Models Used

| Exercise | Model | Technique |
|---|---|---|
| Exercise 2–3 | `gpt-4o-mini` (OpenAI) | Classification, extraction, CoT, self-correction |
| Exercise 4 (sanitizer) | `llama3` via Ollama (local) | PII scrubbing |
| Exercise 4 (sentiment) | `distilbert-base-uncased-finetuned-sst-2-english` (HuggingFace) | Sentiment classification |
| Exercise 4 (urgency) | `facebook/bart-large-mnli` (HuggingFace) | Zero-shot urgency detection |

---

## 10. Future Extensions

The final project concept is an **Event-Driven AI Agent using Event Sourcing and Kafka Streams**.

The architecture for this would extend the existing system as follows:

### Planned Agent Architecture

```
User Event
    │
    ▼
[Agent Input Topic]
    │
    ▼
[Event Store Topic] ◄── append-only log (Event Sourcing)
    │
    ▼
[Kafka Streams Processor]
    ├── State reconstruction from event log
    ├── Intent classification (builds on Exercise 2)
    ├── Tool selection and invocation
    └── Memory aggregation (builds on Exercise 1)
    │
    ▼
[Agent Action Topics]
    ├──► Tool Worker (math, weather, exchange, search)
    ├──► Memory Service (conversation context)
    └──► LLM Worker (generative response)
    │
    ▼
[Agent Output Topic] ──► User Interface
```

### How Current Exercises Map to Final Project

| Final Project Component | Built In |
|---|---|
| Event Sourcing topic (append-only log) | Kafka topics in all exercises |
| Intent router | Exercise 1 (regex) + Exercise 2 (LLM) |
| Prompt engineering pipeline | Exercise 2 (Few-Shot, CoT, Guardrails) |
| Multi-step LLM processing | Exercise 3 (self-correcting pipeline) |
| Parallel inference workers | Exercise 4 (fan-out + stream join) |
| PII sanitization | Exercise 4 (Ollama sanitizer) |
| State reconstruction via Kafka Streams | `kafka-streams-wikimedia` (Java) |
| Memory service | Exercise 1 (MemoryService + history.json) |

### Extension Points

- Replace regex router in Exercise 1 with LLM router from Exercise 2.
- Add an event-sourcing topic that replays to reconstruct agent state.
- Implement a Kafka Streams topology (Java) to aggregate agent memory across sessions.
- Add a tool registry topic where workers advertise their capabilities dynamically.
- Introduce schema registry (Avro/Protobuf) to replace raw JSON string serialization.
- Add dead-letter topics for failed LLM calls and retry logic.
