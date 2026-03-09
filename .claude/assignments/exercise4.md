# Exercise 4: Production-Grade Customer Support Analysis Pipeline

## Overview

Build a distributed AI pipeline using Kafka fan-out and aggregation to analyze customer support messages. The pipeline sanitizes PII, runs parallel sentiment and urgency analysis via local/HuggingFace models, and merges results into actionable insights.

---

## Architecture Overview

```
CLI Input
    │
    ▼
producer.ts
    │
    ▼  [topic: raw-customer-messages]
sanitizer.ts (Bun + Ollama llama3/mistral)
    │
    ▼  [topic: sanitized-messages]
    ├──────────────────────────────────────┐
    │                                      │
    ▼  [consumer group: sentiment-group]   ▼  [consumer group: urgency-group]
sentiment_worker.py                   urgency_worker.py
(distilbert-base-uncased-finetuned)   (facebook/bart-large-mnli)
    │                                      │
    ▼  [topic: analysis-sentiment]         ▼  [topic: analysis-urgency]
    └──────────────┬───────────────────────┘
                   │
                   ▼
          insight-aggregator.ts
          (in-memory Map by message id)
                   │
                   ▼
           Alert / Log Output
```

---

## Kafka Topics

| Topic                  | Producer         | Consumer(s)                              | Description                          |
|------------------------|------------------|------------------------------------------|--------------------------------------|
| `raw-customer-messages`| producer.ts      | sanitizer.ts                             | Raw unfiltered customer input        |
| `sanitized-messages`   | sanitizer.ts     | sentiment_worker.py, urgency_worker.py   | PII-scrubbed messages (fan-out)      |
| `analysis-sentiment`   | sentiment_worker | insight-aggregator.ts                    | Sentiment classification results     |
| `analysis-urgency`     | urgency_worker   | insight-aggregator.ts                    | Urgency classification results       |

---

## Pipeline Flow

```
Step 1  CLI user types a message
Step 2  producer.ts publishes to raw-customer-messages with a unique message_id
Step 3  sanitizer.ts consumes, calls Ollama to scrub PII, publishes to sanitized-messages
Step 4a sentiment_worker.py consumes sanitized-messages (group: sentiment-group)
         → runs distilbert model → publishes label + score to analysis-sentiment
Step 4b urgency_worker.py consumes sanitized-messages (group: urgency-group)
         → runs bart-large-mnli zero-shot → publishes label to analysis-urgency
Step 5  insight-aggregator.ts consumes both analysis topics
         → merges by message_id using in-memory Map
         → when both sides present: applies business logic and emits alert or log
```

---

## Service Responsibilities

### producer.ts (Bun / TypeScript)
- Reads customer message text from CLI (stdin or argv)
- Generates a unique `message_id` (UUID v4)
- Attaches metadata: `timestamp`, `source: "cli"`
- Publishes to `raw-customer-messages`

### sanitizer.ts (Bun / TypeScript + Ollama)
- Consumes `raw-customer-messages`
- Calls local Ollama API (llama3 or mistral) with a PII-scrubbing prompt
- Replaces all proper names with `[NAME]`
- Replaces all phone numbers with `[NUMBER]`
- Preserves original `message_id` for downstream correlation
- Publishes sanitized payload to `sanitized-messages`

### python-workers/sentiment_worker.py (Python + HuggingFace)
- Consumer group: `sentiment-group`
- Consumes `sanitized-messages`
- Loads `distilbert-base-uncased-finetuned-sst-2-english` from HuggingFace
- Classifies message as `POSITIVE` or `NEGATIVE` with confidence score
- Publishes result to `analysis-sentiment`

### python-workers/urgency_worker.py (Python + HuggingFace)
- Consumer group: `urgency-group`
- Consumes `sanitized-messages` independently (fan-out)
- Loads `facebook/bart-large-mnli` for zero-shot classification
- Classifies message into one of: `Urgent`, `Complaint`, `General Inquiry`
- Publishes result to `analysis-urgency`

### insight-aggregator.ts (Bun / TypeScript)
- Consumes `analysis-sentiment` and `analysis-urgency`
- Maintains an in-memory `Map<message_id, PartialInsight>`
- When both sentiment and urgency results arrive for the same `message_id`:
  - Merges into a `FullInsight` object
  - Applies business logic:
    - `sentiment == NEGATIVE` AND `urgency == Urgent` → print strong alert (e.g. `[ALERT]`)
    - Otherwise → print normal log summary
  - Removes entry from Map to free memory

---

## Message Schemas

### raw-customer-messages
```json
{
  "message_id": "uuid-v4-string",
  "timestamp": "ISO-8601",
  "source": "cli",
  "text": "Hello my name is John Doe, call me at 555-1234..."
}
```

### sanitized-messages
```json
{
  "message_id": "uuid-v4-string",
  "timestamp": "ISO-8601",
  "sanitized_text": "Hello my name is [NAME], call me at [NUMBER]..."
}
```

### analysis-sentiment
```json
{
  "message_id": "uuid-v4-string",
  "label": "NEGATIVE",
  "score": 0.97
}
```

### analysis-urgency
```json
{
  "message_id": "uuid-v4-string",
  "label": "Urgent",
  "scores": {
    "Urgent": 0.85,
    "Complaint": 0.10,
    "General Inquiry": 0.05
  }
}
```

### insight-aggregator internal PartialInsight
```json
{
  "message_id": "uuid-v4-string",
  "sanitized_text": "...",
  "sentiment": { "label": "NEGATIVE", "score": 0.97 } | null,
  "urgency": { "label": "Urgent" } | null
}
```

---

## Benchmark Timing Design

Each service should log timestamps at key stages to measure end-to-end latency:

| Stage                          | Log Field          | Description                              |
|--------------------------------|--------------------|------------------------------------------|
| Message published by producer  | `t_produced`       | Kafka publish timestamp                  |
| Sanitizer receives message     | `t_sanitizer_in`   | Kafka consume timestamp                  |
| Sanitizer publishes result     | `t_sanitizer_out`  | After Ollama call completes              |
| Sentiment worker receives      | `t_sentiment_in`   | Kafka consume timestamp                  |
| Sentiment worker publishes     | `t_sentiment_out`  | After model inference                    |
| Urgency worker receives        | `t_urgency_in`     | Kafka consume timestamp                  |
| Urgency worker publishes       | `t_urgency_out`    | After model inference                    |
| Aggregator emits insight       | `t_aggregated`     | When both results merged                 |

The aggregator should compute and print:
- `sanitize_latency_ms = t_sanitizer_out - t_produced`
- `sentiment_latency_ms = t_sentiment_out - t_sanitizer_out`
- `urgency_latency_ms = t_urgency_out - t_sanitizer_out`
- `total_latency_ms = t_aggregated - t_produced`

---

## Implementation Plan (Ordered)

### Phase 1: Infrastructure Setup
1. Create `exercise4/` folder
2. Initialize Bun project (`package.json`, `tsconfig.json`)
3. Initialize Python virtual environment with `requirements.txt`
4. Create Kafka topics:
   - `raw-customer-messages`
   - `sanitized-messages`
   - `analysis-sentiment`
   - `analysis-urgency`
5. Verify Ollama is running locally with llama3 or mistral available

### Phase 2: Producer
6. Implement `producer.ts`
   - UUID generation for `message_id`
   - KafkaJS publish to `raw-customer-messages`
   - CLI input loop

### Phase 3: Sanitizer
7. Implement `sanitizer.ts`
   - Kafka consumer for `raw-customer-messages`
   - Ollama HTTP call with PII-scrubbing prompt
   - Kafka producer to `sanitized-messages`

### Phase 4: Python Workers (in parallel)
8. Implement `python-workers/sentiment_worker.py`
   - HuggingFace pipeline setup
   - Consumer group `sentiment-group`
   - Publish to `analysis-sentiment`
9. Implement `python-workers/urgency_worker.py`
   - HuggingFace pipeline setup
   - Consumer group `urgency-group`
   - Publish to `analysis-urgency`

### Phase 5: Aggregator
10. Implement `insight-aggregator.ts`
    - Dual-topic consumer
    - In-memory Map with partial result merging
    - Business logic for alert vs. normal log
    - Benchmark timing output

### Phase 6: Integration & Testing
11. Run full pipeline end-to-end
12. Test with multiple concurrent messages
13. Verify fan-out: both python workers receive each sanitized message
14. Verify aggregator correctly merges results and applies business logic
15. Measure and log benchmark timings

---

## End-to-End Runtime Scenario

```
Terminal 1:  Start Kafka (local broker on 127.0.0.1:9092)
Terminal 2:  Start Ollama (ollama serve)
Terminal 3:  bun run exercise4/sanitizer.ts
Terminal 4:  python exercise4/python-workers/sentiment_worker.py
Terminal 5:  python exercise4/python-workers/urgency_worker.py
Terminal 6:  bun run exercise4/insight-aggregator.ts
Terminal 7:  bun run exercise4/producer.ts
             > Type: "My name is Jane Smith, call me at 555-9876. This is URGENT!"
```

Expected output in aggregator terminal:
```
[ALERT] message_id=<uuid>
  Text: "My name is [NAME], call me at [NUMBER]. This is URGENT!"
  Sentiment: NEGATIVE (0.97)
  Urgency: Urgent
  sanitize_latency_ms: 1240
  sentiment_latency_ms: 380
  urgency_latency_ms: 520
  total_latency_ms: 1760
```

---

## Folder Structure

```
exercise4/
├── package.json
├── tsconfig.json
├── requirements.txt
├── producer.ts
├── sanitizer.ts
├── insight-aggregator.ts
└── python-workers/
    ├── sentiment_worker.py
    └── urgency_worker.py
```

---

## Key Design Decisions

- **Dedicated raw topic before sanitization**: Ensures the producer is decoupled from AI processing. Raw messages are preserved independently of sanitization failures.
- **Fan-out via consumer groups**: `sanitized-messages` is consumed independently by two worker groups — no coordination needed between them.
- **Correlation by message_id**: Aggregator uses `message_id` as the join key across two independent analysis streams.
- **In-memory Map**: Sufficient for exercise scope; no external state store required.
- **Local models only**: Ollama for PII scrubbing (no cloud API), HuggingFace local inference for analysis workers.
