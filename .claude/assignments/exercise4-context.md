# Exercise 4 Context — Streaming AI Kafka Pipeline

## Overview

Exercise 4 implements a real-time AI processing pipeline using Kafka and multiple microservices written in Node.js and Python.

The system processes customer support messages and analyzes them using AI models.

The architecture follows a streaming event-driven design where each service consumes and produces Kafka events.

The goal is to detect urgent negative customer issues in real time.

---

## Architecture

Pipeline:

```
Producer
→ raw-customer-messages
→ Sanitizer
→ sanitized-messages
→ Sentiment Worker  ─┐
→ Urgency Worker   ─┤
                    └→ Insight Aggregator
```

The pipeline performs parallel AI analysis on sanitized messages and correlates results by message ID in the aggregator.

---

## Kafka Topics

| Topic | Description |
|---|---|
| `raw-customer-messages` | Original customer messages published by the producer |
| `sanitized-messages` | PII-scrubbed messages after Ollama processing; fanned out to both workers |
| `analysis-sentiment` | Sentiment classification results from sentiment_worker.py |
| `analysis-urgency` | Urgency classification results from urgency_worker.py |

---

## Components

### producer.ts

**Language:** Node.js / TypeScript
**File:** `exercise4/producer.ts`

**Role:** CLI interface for sending customer support messages into the pipeline.

**Responsibilities:**
- Read user input from the terminal (interactive or single-shot via argv)
- Generate a UUID for each message
- Publish a JSON payload to `raw-customer-messages` with the UUID as the Kafka key

**Output topic:** `raw-customer-messages`

**Message schema:**
```json
{
  "message_id": "uuid-v4",
  "text": "original customer message",
  "timestamp": "ISO-8601",
  "source": "cli"
}
```

---

### sanitizer.ts

**Language:** Node.js / TypeScript
**File:** `exercise4/sanitizer.ts`

**Role:** Scrubs PII from raw messages before they reach AI workers.

**Responsibilities:**
- Consume messages from `raw-customer-messages` (consumer group: `sanitizer-group`)
- Call the local Ollama API (`POST http://localhost:11434/api/generate`) with a structured prompt
- Replace person names with `[NAME]` and phone numbers with `[NUMBER]`
- Publish the sanitized result preserving the original message ID

**Input topic:** `raw-customer-messages`
**Output topic:** `sanitized-messages`

**Message schema published:**
```json
{
  "id": "original-uuid",
  "text": "sanitized message text",
  "timestamp": "original ISO-8601 timestamp"
}
```

**Environment variables:**
- `OLLAMA_BASE_URL` — default: `http://localhost:11434`
- `OLLAMA_MODEL` — default: `llama3`

---

### sentiment_worker.py

**Language:** Python
**File:** `exercise4/python-workers/sentiment_worker.py`

**Role:** Classifies sentiment of sanitized messages.

**Model:** `distilbert-base-uncased-finetuned-sst-2-english`
**Consumer group:** `sentiment-group`

**Responsibilities:**
- Consume messages from `sanitized-messages`
- Run HuggingFace `pipeline("sentiment-analysis")` on the message text
- Publish the label and confidence score

**Input topic:** `sanitized-messages`
**Output topic:** `analysis-sentiment`

**Message schema published:**
```json
{
  "id": "original-uuid",
  "sentiment": "NEGATIVE",
  "score": 0.999
}
```

**Possible sentiment values:** `POSITIVE`, `NEGATIVE`

---

### urgency_worker.py

**Language:** Python
**File:** `exercise4/python-workers/urgency_worker.py`

**Role:** Classifies urgency level of sanitized messages using zero-shot classification.

**Model:** `facebook/bart-large-mnli`
**Consumer group:** `urgency-group`

**Responsibilities:**
- Consume messages from `sanitized-messages` independently of the sentiment worker
- Run HuggingFace `pipeline("zero-shot-classification")` with three candidate labels
- Publish the top label and its confidence score

**Input topic:** `sanitized-messages`
**Output topic:** `analysis-urgency`

**Candidate labels:** `Urgent`, `Complaint`, `General Inquiry`

**Message schema published:**
```json
{
  "id": "original-uuid",
  "urgency": "Urgent",
  "score": 0.89
}
```

> Both Python workers subscribe to `sanitized-messages` using **different consumer groups** (`sentiment-group` vs `urgency-group`). This is what enables fan-out: Kafka delivers each message to both groups independently.

---

### insight-aggregator.ts

**Language:** Node.js / TypeScript
**File:** `exercise4/insight-aggregator.ts`

**Role:** Correlates sentiment and urgency results by message ID and evaluates the alert rule.

**Consumer group:** `insight-aggregator-group`

**Responsibilities:**
- Subscribe to both `analysis-sentiment` and `analysis-urgency` using a **single Kafka consumer instance** (two `subscribe()` calls before `run()`)
- Store partial results in an in-memory `Map<string, PartialInsight>` keyed by message ID
- When both branches arrive for the same ID, evaluate the alert rule
- Remove the entry from the Map after evaluation to prevent memory growth

**Input topics:** `analysis-sentiment`, `analysis-urgency`

**Alert rule:**

| Sentiment | Urgency | Output |
|---|---|---|
| `NEGATIVE` | `Urgent` | `🚨 STRONG ALERT: customer issue requires immediate attention` |
| any other | any other | `[insight] id=... sentiment=... urgency=...` |

---

## Message Correlation Strategy

The aggregator uses an in-memory `Map<string, PartialInsight>` keyed by message ID.

```
PartialInsight = {
  sentiment?: { sentiment, score }
  urgency?:   { urgency, score }
}
```

When a message arrives from either analysis topic:
1. Upsert the partial result into the Map
2. Check if both `sentiment` and `urgency` are now present
3. If yes — evaluate the alert rule, log the result, delete the Map entry
4. If no — wait for the other branch to arrive

This is a stateful stream join implemented without an external store.

---

## Technologies Used

| Technology | Role |
|---|---|
| Apache Kafka | Message broker and event bus |
| KafkaJS | Kafka client for Node.js services |
| kafka-python | Kafka client for Python workers |
| Bun | TypeScript runtime |
| Node.js / TypeScript | Producer, sanitizer, aggregator |
| Python | Sentiment and urgency workers |
| HuggingFace Transformers | AI model inference |
| Ollama | Local LLM for PII scrubbing |
| Docker | Kafka infrastructure |

---

## File Structure

```
exercise4/
├── producer.ts               — CLI producer
├── sanitizer.ts              — PII scrubbing service
├── insight-aggregator.ts     — Stream join and alert logic
├── prompts.ts                — Ollama prompt templates (stub)
├── kafka/
│   └── kafkaClient.ts        — Shared KafkaJS instance
├── shared/
│   ├── topics.ts             — Topic name constants and consumer group IDs
│   ├── types.ts              — TypeScript interfaces for all message schemas
│   └── benchmark.ts          — Latency computation helpers (stub)
└── python-workers/
    ├── sentiment_worker.py   — distilbert sentiment classifier
    ├── urgency_worker.py     — bart-large-mnli urgency classifier
    └── requirements.txt      — Python dependencies
```

---

## Purpose of This Context File

This file provides persistent architectural context so Claude can understand:

- How the pipeline flows from producer to aggregator
- How services communicate through Kafka topics
- Which consumer groups enable fan-out
- How event correlation works by message ID
- Which files implement which responsibilities

It allows Claude to assist with debugging, extending the system, modifying alert rules, or implementing additional workers without needing to re-read all source files from scratch.
