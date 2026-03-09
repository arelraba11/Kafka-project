# Exercise 4 — Streaming AI Customer Support Analyzer

## Overview

Exercise 4 builds a production-style streaming pipeline that processes customer support messages using local and cloud AI models. Messages flow through five stages: input, PII sanitization, parallel AI classification, and alert aggregation.

Key concepts demonstrated:

- **Stream fan-out** — one sanitized message consumed independently by two worker groups
- **Parallel AI inference** — sentiment and urgency classification run concurrently
- **Event correlation (stream join)** — aggregator merges results by message id using an in-memory Map
- **Real-time alerting** — business rule evaluated as soon as both classification branches arrive

---

## Architecture

```
[User types a message in terminal]
              │
              ▼
         producer.ts
              │
   raw-customer-messages
              │
              ▼
         sanitizer.ts  ←  Ollama (llama3 / mistral)
              │
    sanitized-messages
              │
    ┌─────────┴──────────┐
    ▼                    ▼
sentiment_worker.py  urgency_worker.py
(distilbert)         (bart-large-mnli)
    │                    │
analysis-sentiment  analysis-urgency
    │                    │
    └──────────┬──────────┘
               ▼
    insight-aggregator.ts
    (stream join by message id)
               │
               ▼
   🚨 STRONG ALERT  or  [insight] normal log
```

The aggregator holds partial results in memory keyed by message id. When both branches arrive, it evaluates the alert rule and removes the entry from the Map.

---

## Components

### producer.ts

CLI Kafka producer that reads customer support messages from stdin or a command-line argument and publishes them to `raw-customer-messages`. Each message is assigned a UUID at publish time.

- **Interactive mode:** `bun run producer.ts` — prompts `>` after each message
- **Single-shot mode:** `bun run producer.ts "My internet is down"`

---

### sanitizer.ts

Node.js Kafka consumer that calls a locally running Ollama model to scrub PII before messages reach the AI workers.

Scrubbing rules applied by the model:
- Person names → `[NAME]`
- Phone numbers → `[NUMBER]`

The sanitized message preserves the original id so all downstream services can correlate results.

---

### sentiment_worker.py

Python Kafka consumer that classifies message sentiment using HuggingFace Transformers.

**Model:** `distilbert-base-uncased-finetuned-sst-2-english`
**Consumer group:** `sentiment-group`

**Output schema:**
```json
{
  "id": "...",
  "sentiment": "POSITIVE" | "NEGATIVE",
  "score": 0.97
}
```

---

### urgency_worker.py

Python Kafka consumer that classifies message urgency using zero-shot classification.

**Model:** `facebook/bart-large-mnli`
**Consumer group:** `urgency-group`
**Candidate labels:** `Urgent`, `Complaint`, `General Inquiry`

**Output schema:**
```json
{
  "id": "...",
  "urgency": "Urgent" | "Complaint" | "General Inquiry",
  "score": 0.85
}
```

Both Python workers subscribe to `sanitized-messages` with **different consumer groups**, which enables the fan-out: Kafka delivers each message to both groups independently.

---

### insight-aggregator.ts

Node.js Kafka consumer that subscribes to both analysis topics using a single consumer instance.

**Alert rule:**

| Sentiment | Urgency | Output |
|---|---|---|
| `NEGATIVE` | `Urgent` | `🚨 STRONG ALERT: customer issue requires immediate attention` |
| any other | any other | `[insight] id=... sentiment=... urgency=...` |

After producing the insight log, the entry is deleted from the Map.

---

## Topics

| Topic | Producer | Consumer(s) | Description |
|---|---|---|---|
| `raw-customer-messages` | producer.ts | sanitizer.ts | Raw CLI input |
| `sanitized-messages` | sanitizer.ts | sentiment_worker, urgency_worker | PII-scrubbed text, fanned out |
| `analysis-sentiment` | sentiment_worker | insight-aggregator.ts | Sentiment label and confidence score |
| `analysis-urgency` | urgency_worker | insight-aggregator.ts | Urgency label and confidence score |

---

## Running the Exercise

### 1. Start Kafka

```bash
cd exercise4
docker compose up -d
```

Create the four topics:

```bash
kafka-topics.sh --create --topic raw-customer-messages --bootstrap-server localhost:9092
kafka-topics.sh --create --topic sanitized-messages    --bootstrap-server localhost:9092
kafka-topics.sh --create --topic analysis-sentiment    --bootstrap-server localhost:9092
kafka-topics.sh --create --topic analysis-urgency      --bootstrap-server localhost:9092
```

### 2. Start Ollama

```bash
ollama serve
```

Verify the model is available:

```bash
ollama pull llama3
```

### 3. Install Node.js dependencies

```bash
cd exercise4
bun install
```

### 4. Set up Python environment

```bash
cd exercise4/python-workers
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 5. Start services

Open one terminal per service. Start the aggregator before the producer so no messages are missed.

```bash
# Terminal 1 — Sanitizer
bun run sanitizer.ts

# Terminal 2 — Sentiment worker
cd python-workers && source venv/bin/activate
python sentiment_worker.py

# Terminal 3 — Urgency worker
python urgency_worker.py

# Terminal 4 — Aggregator
bun run insight-aggregator.ts

# Terminal 5 — Producer (type messages here)
bun run producer.ts
```

---

## Example Output

Test messages:

```
My internet is down and I need help immediately
The website is broken and I need urgent support
I really like your product, great job
The billing page is confusing but not urgent
```

**Producer terminal:**
```
[producer] Connected. Type a message and press Enter. Ctrl+C to exit.

> My internet is down and I need help immediately
[producer] Sent message 3f2a1b4c-...
```

**Sanitizer terminal:**
```
[sanitizer] Connected. Waiting for messages...
[sanitizer] received 3f2a1b4c-...
[sanitizer] sanitized 3f2a1b4c-...
```

**Sentiment worker terminal:**
```
[sentiment] received 3f2a1b4c-...
[sentiment] sentiment=NEGATIVE
```

**Urgency worker terminal:**
```
[urgency] received 3f2a1b4c-...
[urgency] urgency=Urgent
```

**Aggregator terminal — alert case:**
```
[insight] received sentiment 3f2a1b4c-...
[insight] received urgency 3f2a1b4c-...

🚨 STRONG ALERT: customer issue requires immediate attention
   id=3f2a1b4c-...  sentiment=NEGATIVE (0.97)  urgency=Urgent (0.85)
```

**Aggregator terminal — normal case:**
```
[insight] received sentiment 7a9c2e1f-...
[insight] received urgency 7a9c2e1f-...
[insight] id=7a9c2e1f-... sentiment=POSITIVE urgency=General Inquiry
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `127.0.0.1:9092` | Kafka broker address |
| `KAFKA_CLIENT_ID` | `exercise4` | KafkaJS client id |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama API base URL |
| `OLLAMA_MODEL` | `llama3` | Ollama model used for PII scrubbing |

---

## Teardown

```bash
docker compose down -v
```
