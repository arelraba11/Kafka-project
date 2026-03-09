# Exercise 2 — LLM Prompt Engineering over Kafka

## Overview

Exercise 2 extends the Exercise 1 distributed chatbot by inserting an LLM-powered classification and extraction pipeline between user input and the domain apps.

Where Exercise 1 used simple regex to classify intent directly in a single router service, Exercise 2 introduces five dedicated microservices that demonstrate four prompt engineering techniques:

| Technique | Service | Purpose |
|---|---|---|
| **Few-Shot Prompting** | LLMRouterService | Classify user intent from labelled examples |
| **Structured JSON Output** | LLMExtractionService | Force the LLM to return typed parameters |
| **Chain-of-Thought Reasoning** | CotMathService | Convert word problems into arithmetic expressions |
| **Persona Prompt** | GeneralChatApp | Enforce a consistent assistant persona |

The Exercise 1 services (MathApp, WeatherApp, ExchangeApp, GeneralChatApp, ResponseAggregator) remain unchanged and are consumed downstream via `function_execution_requests`.

> **LLM integration status:** The `callLLM()` function in each service is a stub that throws intentionally. Services fall back to a regex classifier while the LLM is not connected. The architecture is complete and ready for an LLM client to be plugged in.

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
                   (all intents)             (calculateMath + word problems only)
                              │                   │
                   llm_response_events    cot_math_expression_events
                              │                   │
                   JSONParserService          MathApp (Ex1)
                              │
               ┌──────────────┴─────────────────────────┐
               │          function_execution_requests     │
               ▼                                          ▼
      [Domain apps — Exercise 1]                  bot_output_events
      MathApp / WeatherApp /                             │
      ExchangeApp / GeneralChatApp              ResponseAggregator (Ex1)
```

---

## Kafka Topics

| Topic | Direction | Description |
|---|---|---|
| `user_input_events` | Inbound | User messages from the interface |
| `router_decision_events` | Internal | LLMRouterService intent classification output |
| `llm_prompt_requests` | Internal | Prompts sent to LLM gateway (stub) |
| `llm_response_events` | Internal | Raw LLM string responses |
| `function_execution_requests` | Outbound | Parsed, typed parameters forwarded to apps |
| `bot_output_events` | Outbound | App results before aggregation |
| `guardrail_violation_events` | Outbound | Blocked messages flagged by GuardrailService |
| `cot_math_expression_events` | Internal | CoT-derived arithmetic expressions for MathApp |

---

## Services

### GuardrailService
**File:** `services/guardrailService.ts`
**Consumes:** `user_input_events`
**Produces:** `guardrail_violation_events`

Screens every incoming message for unsafe content (politics, malware). If a violation is detected, publishes a `GuardrailViolationEvent` containing the message:

> "I cannot process this request due to safety protocols."

Safe messages are not re-published — downstream services subscribe to `user_input_events` directly.

Detection logic is a replaceable stub (`detectViolation` returns `null` until implemented).

---

### LLMRouterService
**File:** `services/llmRouterService.ts`
**Consumes:** `user_input_events`
**Produces:** `router_decision_events`

Classifies user intent using a Few-Shot prompt (defined in `prompts/prompts.ts`). Supported intents:

- `getWeather`
- `calculateMath`
- `currencyExchange`
- `generalChat`

**Current behaviour (LLM stub):** `callLLM()` throws, and the service catches the error and falls back to a regex classifier (`regexClassify`). The fallback produces a valid `RouterDecisionEvent` with `confidence: 0.75`. When `callLLM()` is implemented the fallback is bypassed automatically — no code changes are needed in the service logic.

Published event shape:
```json
{
  "userId": "user-1",
  "input": "What is the weather in Berlin?",
  "intent": "getWeather",
  "parameters": { "city": "Berlin" },
  "confidence": 0.75,
  "timestamp": "2026-03-09T10:00:00.000Z"
}
```

---

### LLMExtractionService
**File:** `services/llmExtractionService.ts`
**Consumes:** `router_decision_events`
**Produces:** `llm_response_events`

Builds a structured JSON output prompt for the classified intent and calls the LLM to extract typed parameters. The raw LLM string response is forwarded to `llm_response_events` for parsing by JSONParserService.

The prompt (in `prompts/prompts.ts`) instructs the LLM to return only a JSON object with no surrounding explanation or markdown.

---

### JSONParserService
**File:** `services/jsonParserService.ts`
**Consumes:** `llm_response_events`
**Produces:** `function_execution_requests` (valid JSON) or `bot_output_events` (parse error)

Safely parses the LLM's raw JSON response using `JSON.parse`. On success, publishes a `FunctionExecutionRequestEvent` to `function_execution_requests` so the correct domain app can execute the request. On failure, publishes an error `BotOutputEvent` directly so the user receives feedback without the pipeline stalling.

---

### CotMathService
**File:** `services/cotMathService.ts`
**Consumes:** `router_decision_events`
**Produces:** `cot_math_expression_events`

Activates only when `intent === "calculateMath"` and the input is a natural language word problem (contains alphabetic characters rather than a plain arithmetic expression). Uses a Chain-of-Thought prompt to reason through the problem step by step and output a single arithmetic expression forwarded to MathApp.

Example:
```
Input:  "If I have 5 apples and eat 2 then buy 10"
Output: "5 - 2 + 10"
```

---

## Prompt Templates

All prompts are defined in `prompts/prompts.ts`.

| Function | Technique | Used by |
|---|---|---|
| `llmRouterPrompt(input)` | Few-Shot Prompting (3+ examples per intent) | LLMRouterService |
| `llmExtractionPrompt(intent, input)` | Structured JSON Output | LLMExtractionService |
| `cotMathPrompt(problem)` | Chain-of-Thought Reasoning | CotMathService |
| `generalChatPersonaPrompt` | Persona Prompt | GeneralChatApp |

---

## Running the System

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

## Test Inputs

Inject messages directly into the pipeline using the Kafka console producer:

```bash
docker exec -it kafka-ex2 kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic user_input_events \
  --property "parse.key=true" \
  --property "key.separator=:"
```

Type messages in `key:json` format, then press Enter:

**Weather query**
```
user-1:{"userId":"user-1","userInput":"What is the weather in Berlin?","timestamp":"2026-03-09T10:00:00.000Z"}
```

**Math calculation**
```
user-1:{"userId":"user-1","userInput":"42 * 7","timestamp":"2026-03-09T10:01:00.000Z"}
```

**Math word problem (triggers CotMathService)**
```
user-1:{"userId":"user-1","userInput":"If I have 5 apples and eat 2 then buy 10","timestamp":"2026-03-09T10:02:00.000Z"}
```

**Currency exchange**
```
user-1:{"userId":"user-1","userInput":"Convert 100 USD to EUR","timestamp":"2026-03-09T10:03:00.000Z"}
```

**General chat**
```
user-1:{"userId":"user-1","userInput":"Hello how are you?","timestamp":"2026-03-09T10:04:00.000Z"}
```

Press `Ctrl+C` to exit the producer.

---

## Observing the Pipeline

Open a consumer for each topic to observe message flow in real time.

**Intent classification output:**
```bash
docker exec kafka-ex2 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic router_decision_events \
  --from-beginning \
  --property print.key=true
```

**LLM extraction output:**
```bash
docker exec kafka-ex2 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic llm_response_events \
  --from-beginning \
  --property print.key=true
```

**Final routing to apps:**
```bash
docker exec kafka-ex2 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic function_execution_requests \
  --from-beginning \
  --property print.key=true
```

**Guardrail violations:**
```bash
docker exec kafka-ex2 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic guardrail_violation_events \
  --from-beginning \
  --property print.key=true
```

**Expected service log output per message (LLMRouterService):**
```
[llm-router] Message received — userId=user-1 input="What is the weather in Berlin?"
[llm-router] LLM unavailable, using regex fallback. Reason: LLM not yet integrated — implement callLLM()
[llm-router] Detected intent="getWeather" confidence=0.75 parameters={"city":"Berlin"}
[llm-router] Published to router_decision_events — userId=user-1 intent=getWeather
```

---

## Teardown

```bash
docker compose down -v
```
