# Exercise 2 — Kafka Integration with LLM

## Overview

Exercise 2 extends the Exercise 1 chatbot by inserting an LLM-powered classification and extraction pipeline between user input and the domain apps. Where Exercise 1 used simple regex to classify intent in a single router service, Exercise 2 introduces five dedicated microservices that demonstrate four prompt engineering techniques.

| Technique | Service | Purpose |
|---|---|---|
| **Few-Shot Prompting** | LLMRouterService | Classify user intent from labelled examples |
| **Structured JSON Output** | LLMExtractionService | Force the LLM to return typed parameters |
| **Chain-of-Thought Reasoning** | CotMathService | Convert word problems into arithmetic expressions |
| **Persona Prompt** | GeneralChatApp | Enforce a consistent assistant persona |

The Exercise 1 domain services (MathApp, WeatherApp, ExchangeApp, GeneralChatApp, ResponseAggregator) remain unchanged and are consumed downstream via `function_execution_requests`.

> **Note:** The `callLLM()` function in each service is a stub. Services fall back to a regex classifier while the LLM is not connected. The architecture is complete and ready for an LLM client to be plugged in.

---

## Architecture

```
user_input_events
        │
        ├──▶ GuardrailService ──────────▶ guardrail_violation_events
        │                                 (blocked inputs only)
        │
        └──▶ LLMRouterService ──────────▶ router_decision_events
                                                  │
                              ┌───────────────────┤
                              │                   │
                              ▼                   ▼
                   LLMExtractionService      CotMathService
                   (all intents)             (math word problems only)
                              │                   │
                   llm_response_events    cot_math_expression_events
                              │                   │
                   JSONParserService          MathApp (Ex1)
                              │
               ┌──────────────┴──────────────────────────┐
               │         function_execution_requests       │
               ▼                                           ▼
      [Domain apps — Exercise 1]                 bot_output_events
      MathApp / WeatherApp /                             │
      ExchangeApp / GeneralChatApp             ResponseAggregator (Ex1)
```

---

## Components

### GuardrailService
**File:** `services/guardrailService.ts`

Screens every incoming message for unsafe content (politics, malware). If a violation is detected, publishes a `GuardrailViolationEvent` with a refusal message. Safe messages are not re-published — downstream services subscribe to `user_input_events` directly.

---

### LLMRouterService
**File:** `services/llmRouterService.ts`

Classifies user intent using a Few-Shot prompt. Supported intents: `getWeather`, `calculateMath`, `currencyExchange`, `generalChat`. Falls back to a regex classifier when the LLM is unavailable, producing a valid `RouterDecisionEvent` with `confidence: 0.75`.

---

### LLMExtractionService
**File:** `services/llmExtractionService.ts`

Builds a structured JSON output prompt for the classified intent and calls the LLM to extract typed parameters. The raw LLM string response is forwarded to `llm_response_events` for parsing by JSONParserService.

---

### JSONParserService
**File:** `services/jsonParserService.ts`

Parses the LLM's raw JSON response. On success, publishes a `FunctionExecutionRequestEvent` to the correct domain app. On failure, publishes an error `BotOutputEvent` so the user receives feedback without the pipeline stalling.

---

### CotMathService
**File:** `services/cotMathService.ts`

Activates when `intent === "calculateMath"` and the input is a natural language word problem. Uses a Chain-of-Thought prompt to reason through the problem step by step and output a single arithmetic expression for MathApp.

**Example:**
```
Input:  "If I have 5 apples and eat 2 then buy 10"
Output: "5 - 2 + 10"
```

---

## Topics

| Topic | Producer | Consumer(s) | Description |
|---|---|---|---|
| `user_input_events` | (external) | GuardrailService, LLMRouterService | Inbound user messages |
| `router_decision_events` | LLMRouterService | LLMExtractionService, CotMathService | Intent classification output |
| `llm_response_events` | LLMExtractionService | JSONParserService | Raw LLM string responses |
| `function_execution_requests` | JSONParserService | Domain apps (Ex1) | Typed parameters for domain apps |
| `cot_math_expression_events` | CotMathService | MathApp | CoT-derived arithmetic expressions |
| `bot_output_events` | Domain apps | ResponseAggregator | App results before aggregation |
| `guardrail_violation_events` | GuardrailService | (log only) | Blocked messages |

---

## Running the Exercise

### 1. Start Kafka

```bash
cd exercise2
docker compose up -d
```

Confirm the broker is ready:

```bash
docker logs kafka-ex2 --tail 20
# Look for: "Kafka Server started"
```

### 2. Create topics

```bash
chmod +x topics.sh
bash topics.sh
```

Verify:

```bash
docker exec kafka-ex2 kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 3. Install dependencies

```bash
bun install
```

### 4. Start services

Open a separate terminal for each service:

```bash
# Terminal 1
bun services/guardrailService.ts

# Terminal 2
bun services/llmRouterService.ts

# Terminal 3
bun services/llmExtractionService.ts

# Terminal 4
bun services/jsonParserService.ts

# Terminal 5
bun services/cotMathService.ts
```

---

## Example Output

Inject a test message using the Kafka console producer:

```bash
docker exec -it kafka-ex2 kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic user_input_events \
  --property "parse.key=true" \
  --property "key.separator=:"
```

Type a message in `key:json` format:

```
user-1:{"userId":"user-1","userInput":"What is the weather in Berlin?","timestamp":"2026-03-09T10:00:00.000Z"}
```

Expected LLMRouterService log:

```
[llm-router] Message received — userId=user-1 input="What is the weather in Berlin?"
[llm-router] LLM unavailable, using regex fallback.
[llm-router] Detected intent="getWeather" confidence=0.75 parameters={"city":"Berlin"}
[llm-router] Published to router_decision_events — userId=user-1 intent=getWeather
```

Observe the full topic event flow by attaching console consumers to any topic listed in the Topics section.

---

## Submission Artifacts

The following logs must be captured and submitted to demonstrate each feature:

| Feature | What to show |
|---|---|
| Topic event flow | Messages flowing through at least two topics end-to-end |
| Few-Shot routing | Router log showing intent classification |
| JSON output | JSONParserService successfully parsing structured parameters |
| Chain-of-thought math | CotMathService log showing step-by-step reasoning |
| Guardrail refusal | GuardrailService blocking a flagged input |

---

## Teardown

```bash
docker compose down -v
```
