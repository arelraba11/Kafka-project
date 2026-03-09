# Exercise 2 — LLM Prompt Engineering over Kafka (Context Summary)

## Stack
- Runtime: Bun (TypeScript)
- Messaging: Kafka via KafkaJS (`localhost:9092`)
- Infrastructure: Docker Compose (`bitnami/kafka:3.5`, KRaft mode)
- Root: `exercise2/`

---

## 1. System Overview

Exercise 2 extends the Exercise 1 distributed chatbot by inserting an LLM-powered classification and
extraction layer between the user input and the domain apps.

Instead of the Exercise 1 router using simple regex to dispatch directly to intent topics, Exercise 2
introduces a pipeline of dedicated microservices that:

1. Screen the input for safety violations (guardrail)
2. Classify the intent using Few-Shot prompting (LLM router)
3. Extract structured parameters using JSON output prompting (LLM extraction)
4. Parse and validate the LLM response before forwarding it to the apps (JSON parser)
5. Convert natural language math word problems into arithmetic expressions using Chain-of-Thought
   prompting (CoT math)

The Exercise 1 services (mathApp, weatherApp, exchangeApp, generalChatApp, responseAggregator) remain
unchanged and are consumed downstream via `function_execution_requests`.

---

## 2. Topics Used

| Topic name                   | Constant                      | Direction                                  |
|------------------------------|-------------------------------|--------------------------------------------|
| `user_input_events`          | `USER_INPUT_EVENTS`           | Inbound — produced by UserInterface (Ex1)  |
| `router_decision_events`     | `ROUTER_DECISION_EVENTS`      | GuardrailService + LLMRouterService output |
| `llm_prompt_requests`        | `LLM_PROMPT_REQUESTS`         | LLMExtractionService → LLM gateway (stub)  |
| `llm_response_events`        | `LLM_RESPONSE_EVENTS`         | LLMExtractionService output                |
| `function_execution_requests`| `FUNCTION_EXECUTION_REQUESTS` | JSONParserService → domain apps            |
| `bot_output_events`          | `BOT_OUTPUT_EVENTS`           | Domain apps → ResponseAggregator           |
| `guardrail_violation_events` | `GUARDRAIL_VIOLATION_EVENTS`  | GuardrailService output on blocked input   |
| `cot_math_expression_events` | `COT_MATH_EXPRESSION_EVENTS`  | CotMathService → mathApp                   |

Topics are created via `topics.sh` using `docker exec kafka-ex2 kafka-topics.sh`.

---

## 3. Services Implemented

### guardrailService
```
Consumes:  user_input_events
Produces:  guardrail_violation_events  (on violation only)
Group ID:  guardrail-service
```
Screens every incoming message for unsafe topics (politics, malware). If a violation is detected,
it publishes a `GuardrailViolationEvent` with the message:
`"I cannot process this request due to safety protocols."`
Safe messages are not re-published — downstream services consume `user_input_events` directly.

Detection logic is stubbed (`detectViolation` returns `null`). Replace with real logic in the
implementation phase.

---

### llmRouterService
```
Consumes:  user_input_events
Produces:  router_decision_events
Group ID:  llm-router-service
```
Classifies user intent using Few-Shot prompting. Supported intents:
`getWeather | calculateMath | currencyExchange | generalChat`

The LLM is not yet integrated. `callLLM()` throws intentionally. The service catches the error and
falls back to a regex classifier (`regexClassify`) that produces a valid `RouterDecisionEvent` with
`confidence: 0.75`. When `callLLM()` is implemented the fallback is bypassed automatically.

Publishes `RouterDecisionEvent`:
```ts
{
  userId: string
  input: string
  intent: LLMIntent
  parameters: Record<string, unknown>
  confidence: number
  timestamp: string
}
```

---

### llmExtractionService
```
Consumes:  router_decision_events
Produces:  llm_response_events
Group ID:  llm-extraction-service
```
Builds a structured JSON output prompt for the classified intent and calls the LLM to extract typed
parameters. The raw LLM string response is forwarded as-is to `llm_response_events` for parsing
downstream.

LLM call is stubbed — `rawResponse` is currently `"{}"`.

---

### jsonParserService
```
Consumes:  llm_response_events
Produces:  function_execution_requests  (on valid JSON)
           bot_output_events            (on parse error)
Group ID:  json-parser-service
```
Safely parses the raw LLM JSON response. On success, publishes a `FunctionExecutionRequestEvent`
to `function_execution_requests` so the correct domain app can execute. On failure, publishes an
error `BotOutputEvent` directly to `bot_output_events`.

---

### cotMathService
```
Consumes:  router_decision_events  (intent = calculateMath only)
Produces:  cot_math_expression_events
Group ID:  cot-math-service
```
Triggers only when `intent === "calculateMath"` and the input is a natural language word problem
(contains letters, not a plain arithmetic expression). Uses Chain-of-Thought prompting to convert
the word problem into an arithmetic expression forwarded to the mathApp.

LLM call is stubbed — `expression` and `reasoning` are currently empty strings.

---

## 4. Event Flow (Pipeline)

```
[User types]
     |
user_input_events
     |
     +----> GuardrailService -------> guardrail_violation_events
     |                                (blocked inputs only)
     |
     +----> LLMRouterService
                |
         router_decision_events
                |
                +----> LLMExtractionService
                |           |
                |      llm_response_events
                |           |
                |      JSONParserService
                |           |
                |      function_execution_requests
                |           |
                |      [domain apps — Exercise 1]
                |           |
                |      bot_output_events
                |           |
                |      ResponseAggregator (Exercise 1)
                |
                +----> CotMathService (calculateMath + word problem only)
                            |
                       cot_math_expression_events
                            |
                       mathApp (Exercise 1)
```

---

## 5. Router Logic

The LLM is not yet integrated. `llmRouterService` uses a regex fallback classifier while
`callLLM()` remains a stub.

### Regex fallback rules (in priority order)

| Intent            | Match condition                                                          |
|-------------------|--------------------------------------------------------------------------|
| `calculateMath`   | Input contains a digit-operator-digit pattern or "calculate/compute/what is \<digit\>" |
| `getWeather`      | Input contains: weather, temperature, forecast, rain, sunny, hot, cold   |
| `currencyExchange`| Input contains a currency code (USD, EUR, ILS, GBP, JPY, CHF, CAD, AUD) or "convert/exchange" |
| `generalChat`     | Default fallback (no other pattern matched)                              |

All regex classifications use `confidence: 0.75` (or `0.70` for `generalChat`).

When `callLLM()` is implemented, the `try/catch` in `classifyIntent()` will prefer the LLM result
and the regex branch will only run on LLM failure — no service changes needed.

### Prompt templates (exercise2/prompts/prompts.ts)

| Function                     | Technique              | Used by                |
|------------------------------|------------------------|------------------------|
| `llmRouterPrompt(input)`     | Few-Shot Prompting     | llmRouterService       |
| `llmExtractionPrompt(intent, input)` | Structured JSON Output | llmExtractionService |
| `cotMathPrompt(problem)`     | Chain-of-Thought       | cotMathService         |
| `generalChatPersonaPrompt`   | Persona Prompt         | generalChatApp         |

---

## 6. Example Test Inputs

Inject via Kafka console producer:

```bash
docker exec -it kafka-ex2 kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic user_input_events \
  --property "parse.key=true" \
  --property "key.separator=:"
```

**Weather**
```
user-1:{"userId":"user-1","userInput":"What is the weather in Berlin?","timestamp":"2026-03-09T10:00:00.000Z"}
```
Expected intent: `getWeather` | parameters: `{ "city": "Berlin" }`

**Math (direct expression)**
```
user-1:{"userId":"user-1","userInput":"2 + 2","timestamp":"2026-03-09T10:01:00.000Z"}
```
Expected intent: `calculateMath` | parameters: `{ "expression": "2 + 2" }`

**Math (word problem — triggers cotMathService)**
```
user-1:{"userId":"user-1","userInput":"If I have 5 apples and eat 2 then buy 10","timestamp":"2026-03-09T10:02:00.000Z"}
```
Expected intent: `calculateMath` + `isWordProblem=true` → routed to `cot_math_expression_events`

**Currency**
```
user-1:{"userId":"user-1","userInput":"Convert 100 USD to EUR","timestamp":"2026-03-09T10:03:00.000Z"}
```
Expected intent: `currencyExchange` | parameters: `{ "from": "USD", "to": "EUR" }`

**General chat**
```
user-1:{"userId":"user-1","userInput":"Hello how are you?","timestamp":"2026-03-09T10:04:00.000Z"}
```
Expected intent: `generalChat` | parameters: `{}`

---

## 7. Verification

Open a consumer for each output topic in a separate terminal to observe message flow.

### Verify intent classification (llmRouterService output)
```bash
docker exec kafka-ex2 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic router_decision_events \
  --from-beginning \
  --property print.key=true
```

### Verify LLM extraction output (llmExtractionService output)
```bash
docker exec kafka-ex2 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic llm_response_events \
  --from-beginning \
  --property print.key=true
```

### Verify final routing to apps (jsonParserService output)
```bash
docker exec kafka-ex2 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic function_execution_requests \
  --from-beginning \
  --property print.key=true
```

### Verify guardrail blocks
```bash
docker exec kafka-ex2 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic guardrail_violation_events \
  --from-beginning \
  --property print.key=true
```

### Expected service log output (per message)
```
[llm-router] Message received — userId=user-1 input="What is the weather in Berlin?"
[llm-router] LLM unavailable, using regex fallback. Reason: LLM not yet integrated — implement callLLM()
[llm-router] Detected intent="getWeather" confidence=0.75 parameters={"city":"Berlin"}
[llm-router] Published to router_decision_events — userId=user-1 intent=getWeather
```

---

## How to Run

```bash
# 1. Start Kafka
cd exercise2
docker compose up -d

# 2. Install dependencies (once)
bun install

# 3. Create topics (once)
bash topics.sh

# 4. Start services — each in a separate terminal
bun services/guardrailService.ts
bun services/llmRouterService.ts
bun services/llmExtractionService.ts
bun services/jsonParserService.ts
bun services/cotMathService.ts
```

---

## Debugging Notes

### Silent message drop — fixed in llmRouterService (2026-03-09)

**Symptom:** Messages arrived in `user_input_events` but nothing appeared in `router_decision_events`.

**Root cause:** `classifyIntent()` unconditionally returned `null`. The handler's `if (!decision) return`
silently discarded every message. `callLLM()` threw but was never called, so no error surfaced.

**Fix:** `classifyIntent()` now calls `callLLM()` inside a `try/catch`. On failure it falls back to
`regexClassify()` which always returns a valid result. The handler never receives `null` and always
publishes to `router_decision_events`.
