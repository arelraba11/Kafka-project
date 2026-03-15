# Exercise 2 → Main Architecture: Simplified Integration Plan

## Context

Exercise 1 was refactored into `shared/`, `services/`, and `infra/`. Exercise 2 is still a standalone folder with its own Kafka client, topics, types, and LLM pipeline. This plan migrates Exercise 2 into the main architecture, with the constraint that `.env` already has `OPENAI_API_KEY` and `ROUTER_MODE=llm`, and `package.json` already has the `openai` dependency.

The design principle: **reuse every existing topic possible, add only what is structurally necessary.**

---

## Topic Elimination Analysis

| Exercise 2 Topic | Decision | Reason |
|---|---|---|
| `user_input_events` | **Reuse** `user-input-events` | Same role, already exists |
| `router_decision_events` | **New** `router-decision-events` | Required bridge from routerService (LLM mode) to LLM services |
| `guardrail_violation_events` | **New** `guardrail-violation-events` | Required audit log for blocked inputs |
| `llm_prompt_requests` | **Eliminated** | Never produced or consumed — pure stub in exercise2 |
| `llm_response_events` | **Eliminated** | Intermediate hop between llmExtractionService and jsonParserService — merge both into one service |
| `function_execution_requests` | **Eliminated** | Replaced by direct mapping to existing `intent-*` topics |
| `cot_math_expression_events` | **Eliminated** | No consumer; CoT reasoning logged locally instead |
| `bot_output_events` | **Eliminated** | Reuse existing `app-results` for error path |

**Result: 2 new topics total** (down from 5 in the draft plan).

---

## Service Consolidation

`llmExtractionService` and `jsonParserService` are a thin two-hop chain with no fan-out between them:
- `llmExtractionService`: receives `RouterDecisionEvent` → calls LLM → wraps in `LLMResponseEvent`
- `jsonParserService`: receives `LLMResponseEvent` → parses JSON → routes to `intent-*`

Merging them eliminates `llm-response-events` and reduces the service count. The merged service is named `llm-router-service`: it consumes `router-decision-events`, calls the LLM, parses the JSON, and routes directly to the correct `intent-*` topic.

---

## Final Kafka Topic List

### Existing topics (unchanged)

| Topic | Key |
|---|---|
| `user-input-events` | `TOPICS.USER_INPUT` |
| `user-control-events` | `TOPICS.USER_CONTROL` |
| `intent-math` | `TOPICS.INTENT_MATH` |
| `intent-weather` | `TOPICS.INTENT_WEATHER` |
| `intent-exchange` | `TOPICS.INTENT_EXCHANGE` |
| `intent-general-chat` | `TOPICS.INTENT_CHAT` |
| `app-results` | `TOPICS.APP_RESULTS` |
| `bot-responses` | `TOPICS.BOT_RESPONSES` |
| `conversation-history-update` | `TOPICS.HISTORY_UPDATE` |

### New topics (2 only)

| Topic | Key | Purpose |
|---|---|---|
| `router-decision-events` | `TOPICS.ROUTER_DECISION` | Bridge: routerService (LLM mode) → llm-router-service + cot-math-service |
| `guardrail-violation-events` | `TOPICS.GUARDRAIL_VIOLATION` | Audit log: blocked/unsafe user inputs |

---

## Architecture: LLM vs Regex Mode

### ROUTER_MODE=regex (Exercise 1 path — fully preserved, zero changes to apps)

```
user-input-events
  ├──► guardrail-service (passive auditor) ──► guardrail-violation-events
  └──► router-service [regex branch, unchanged logic]
            ├──► intent-math ──► mathApp ──► app-results ──► responseAggregator ──► bot-responses
            ├──► intent-weather ──► weatherApp ──► app-results
            ├──► intent-exchange ──► exchangeApp ──► app-results
            └──► intent-general-chat ──► generalChatApp ──► app-results
```

### ROUTER_MODE=llm (new LLM path)

```
user-input-events
  ├──► guardrail-service ──► guardrail-violation-events
  └──► router-service [llm branch] ──► router-decision-events
            ├──► llm-router-service (extract + parse + route)
            │         ├──► intent-weather ──► weatherApp ──► app-results
            │         ├──► intent-exchange ──► exchangeApp ──► app-results
            │         ├──► intent-general-chat ──► generalChatApp ──► app-results
            │         └──► (error) ──► app-results [AppResultEvent success:false]
            └──► cot-math-service (calculateMath + word problems only)
                      └──► intent-math ──► mathApp ──► app-results
                      [CoT reasoning logged locally, not to Kafka]
```

The `responseAggregator`, all four apps, `memoryService`, and `userInterface` are **untouched in both modes**.

---

## Target Directory Structure

```
/
├── shared/
│   ├── kafka/client.ts                    [UNCHANGED]
│   ├── llm/
│   │   └── openai.ts                      [NEW] — OpenAI singleton + callLLM()
│   ├── prompts/
│   │   └── prompts.ts                     [NEW] — copied from exercise2/prompts/prompts.ts
│   ├── topics.ts                          [MODIFIED] — +2 new keys
│   └── types/
│       ├── conversation.ts                [UNCHANGED]
│       └── events.ts                      [MODIFIED] — +LLMIntent + 3 new event types
│
├── services/
│   ├── user-interface/userInterface.ts    [UNCHANGED]
│   ├── memory-service/memoryService.ts    [UNCHANGED]
│   ├── response-aggregator/
│   │   └── responseAggregator.ts          [UNCHANGED]
│   ├── router-service/
│   │   └── routerService.ts               [MODIFIED] — add ROUTER_MODE=llm branch
│   ├── guardrail-service/
│   │   └── guardrailService.ts            [NEW]
│   ├── llm-router-service/
│   │   └── llmRouterService.ts            [NEW] — merges llmExtractionService + jsonParserService
│   ├── cot-math-service/
│   │   └── cotMathService.ts              [NEW]
│   └── apps/
│       ├── mathApp.ts                     [UNCHANGED]
│       ├── weatherApp.ts                  [UNCHANGED]
│       ├── exchangeApp.ts                 [UNCHANGED]
│       └── generalChatApp.ts              [UNCHANGED]
│
├── infra/
│   ├── docker-compose.yml                 [UNCHANGED]
│   └── topics.sh                          [MODIFIED] — append 2 new topics
│
├── package.json                           [UNCHANGED — openai already present]
├── tsconfig.json                          [UNCHANGED — shared/**/* + services/**/* covers new paths]
└── .env                                   [UNCHANGED — OPENAI_API_KEY + ROUTER_MODE already present]
```

**New files: 5** (`shared/llm/openai.ts`, `shared/prompts/prompts.ts`, `services/guardrail-service/guardrailService.ts`, `services/llm-router-service/llmRouterService.ts`, `services/cot-math-service/cotMathService.ts`)
**Modified files: 4** (`shared/topics.ts`, `shared/types/events.ts`, `services/router-service/routerService.ts`, `infra/topics.sh`)

---

## Detailed File Specifications

### 1. `shared/topics.ts` — MODIFIED

Append 2 keys to the existing `TOPICS` object. All 9 existing keys stay untouched:

```typescript
ROUTER_DECISION:    "router-decision-events",
GUARDRAIL_VIOLATION:"guardrail-violation-events",
```

### 2. `shared/types/events.ts` — MODIFIED

Append after existing types (no rewrites):

```typescript
export type LLMIntent = "getWeather" | "calculateMath" | "currencyExchange" | "generalChat";

export interface RouterDecisionEvent {
  userId: string;
  input: string;
  intent: LLMIntent;
  parameters: Record<string, unknown>;
  confidence: number;
  timestamp: string;
}

export interface GuardrailViolationEvent {
  userId: string;
  userInput: string;
  violationType: "politics" | "malware";
  message: string;
  timestamp: string;
}

export interface CotMathExpressionEvent {  // for local logging only, not produced to Kafka
  userId: string;
  originalInput: string;
  expression: string;
  reasoning: string;
  timestamp: string;
}
```

### 3. `shared/llm/openai.ts` — NEW

Singleton + helper. Strips markdown code fences that LLMs sometimes include:

```typescript
import OpenAI from "openai";

export const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

export async function callLLM(prompt: string): Promise<string> {
  const res = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [{ role: "user", content: prompt }],
  });
  const content = res.choices[0].message.content ?? "";
  return content.replace(/^```json\n?/, "").replace(/\n?```$/, "").trim();
}
```

### 4. `shared/prompts/prompts.ts` — NEW

Direct copy of `exercise2/prompts/prompts.ts` (no imports in that file, trivial relocation). Contains:
- `llmRouterPrompt(userInput)` — Few-Shot intent classification
- `llmExtractionPrompt(intent, userInput)` — Structured JSON parameter extraction
- `cotMathPrompt(wordProblem)` — Chain-of-Thought math expression derivation
- `generalChatPersonaPrompt` — Persona constant for GeneralChatApp
- Add: `guardrailPrompt(userInput)` — Instructs LLM to classify input as safe or return violation type

### 5. `services/router-service/routerService.ts` — MODIFIED

Add `ROUTER_MODE` check at the top of the message handler. The existing regex logic is completely unchanged inside its branch:

```typescript
// New at top of file:
import { callLLM } from "../../shared/llm/openai";
import { llmRouterPrompt } from "../../shared/prompts/prompts";
import type { RouterDecisionEvent, LLMIntent } from "../../shared/types/events";

const ROUTER_MODE = process.env.ROUTER_MODE ?? "regex";

// New helper (lifted from exercise2/services/llmRouterService.ts):
async function classifyWithLLM(input: string): Promise<RouterDecisionEvent fields> {
  try {
    const raw = await callLLM(llmRouterPrompt(input));
    return JSON.parse(raw); // { intent, parameters, confidence }
  } catch {
    return regexClassify(input); // existing classify() renamed to regexClassify()
  }
}

// In message handler — wrap existing switch in if/else:
if (ROUTER_MODE === "llm") {
  const { intent, parameters, confidence } = await classifyWithLLM(userInput);
  const payload: RouterDecisionEvent = { userId, input: userInput, intent, parameters, confidence, timestamp: new Date().toISOString() };
  await sendMessage(producer, TOPICS.ROUTER_DECISION, userId, payload);
  return; // llm-router-service and cot-math-service take it from here
}
// else: existing regex switch → intent-* topics (unchanged)
```

The existing `classify()` function is renamed `regexClassify()`. No other changes.

### 6. `services/guardrail-service/guardrailService.ts` — NEW

Source: `exercise2/services/guardrailService.ts` with updated imports and real `detectViolation`:

```
Consumes: TOPICS.USER_INPUT  (consumer group: "guardrail-service")
Produces: TOPICS.GUARDRAIL_VIOLATION  (only on violations)
```

`detectViolation(input)` replaced with a real `callLLM(guardrailPrompt(input))` call. Response parsed to determine if violation and what type. Safe inputs silently pass (no re-publication needed; downstream services consume the original topic independently).

### 7. `services/llm-router-service/llmRouterService.ts` — NEW (merges extraction + parsing)

This service replaces both `llmExtractionService` and `jsonParserService` from exercise2:

```
Consumes: TOPICS.ROUTER_DECISION  (consumer group: "llm-router-service")
Produces: TOPICS.INTENT_MATH | TOPICS.INTENT_WEATHER | TOPICS.INTENT_EXCHANGE | TOPICS.INTENT_CHAT
          TOPICS.APP_RESULTS  (on parse error — AppResultEvent { success: false })
```

Logic:
1. Receive `RouterDecisionEvent`
2. Skip if `intent === "calculateMath"` (cotMathService owns all math)
3. Call `callLLM(llmExtractionPrompt(intent, input))`
4. Parse JSON response → extract `parameters`
5. Map intent → correct `intent-*` topic with typed event:
   - `getWeather` → `IntentWeatherEvent { userId, city, timestamp }` → `TOPICS.INTENT_WEATHER`
   - `currencyExchange` → `IntentExchangeEvent { userId, currencyCode, targetCurrency, timestamp }` → `TOPICS.INTENT_EXCHANGE`
   - `generalChat` → `IntentGeneralChatEvent { userId, userInput, context: [], timestamp }` → `TOPICS.INTENT_CHAT`
6. On JSON parse failure → `AppResultEvent { success: false, result: "Sorry..." }` → `TOPICS.APP_RESULTS`

### 8. `services/cot-math-service/cotMathService.ts` — NEW

```
Consumes: TOPICS.ROUTER_DECISION  (consumer group: "cot-math-service")
Produces: TOPICS.INTENT_MATH
```

Logic:
1. Receive `RouterDecisionEvent`
2. Skip if `intent !== "calculateMath"`
3. If input is a word problem (contains letters, not just digits/operators) → call `callLLM(cotMathPrompt(input))`
4. Parse JSON → `{ reasoning, expression }`
5. Log reasoning locally: `console.log("[cot-math] reasoning:", reasoning)`
6. Publish `IntentMathEvent { userId, expression, timestamp }` → `TOPICS.INTENT_MATH`
7. If input is already a pure arithmetic expression → skip LLM, publish directly with expression = input

### 9. `infra/topics.sh` — MODIFIED

Append 2 entries to the existing topic creation list:

```bash
"router-decision-events"
"guardrail-violation-events"
```

---

## Step-by-Step Migration Order

### Phase 1 — Shared infrastructure (no service changes)

1. **`shared/topics.ts`** — Add `ROUTER_DECISION` and `GUARDRAIL_VIOLATION` keys
2. **`shared/types/events.ts`** — Append `LLMIntent`, `RouterDecisionEvent`, `GuardrailViolationEvent`, `CotMathExpressionEvent`
3. **`shared/llm/openai.ts`** — Create OpenAI singleton + `callLLM()`
4. **`shared/prompts/prompts.ts`** — Copy from `exercise2/prompts/prompts.ts`, add `guardrailPrompt()`

### Phase 2 — New pure services (no changes to existing services)

5. **`services/guardrail-service/guardrailService.ts`** — New, depends on phase 1
6. **`services/cot-math-service/cotMathService.ts`** — New, depends on phase 1
7. **`services/llm-router-service/llmRouterService.ts`** — New, depends on phase 1

### Phase 3 — Modify one existing service

8. **`services/router-service/routerService.ts`** — Add `ROUTER_MODE=llm` branch; rename `classify()` → `regexClassify()`

### Phase 4 — Infrastructure

9. **`infra/topics.sh`** — Append 2 new topic names

---

## Invariants Preserved

| What | How |
|---|---|
| All 4 apps unchanged | llm-router-service maps to their existing `intent-*` topics with correctly shaped events |
| responseAggregator unchanged | Error path publishes to `TOPICS.APP_RESULTS` which it already consumes |
| memoryService unchanged | Reads `USER_INPUT` + `APP_RESULTS` — both unchanged |
| userInterface unchanged | Reads `BOT_RESPONSES` — unchanged |
| All 9 existing topic names unchanged | Only 2 additions, zero renames |
| `ROUTER_MODE=regex` is identical to today | Existing regex branch is untouched inside the `if` guard |
| No new npm dependencies | `openai` already in `package.json` |

---

## Verification

```bash
# 1. Create the 2 new topics
bash infra/topics.sh

# 2. Test ROUTER_MODE=regex — should behave identically to current system
ROUTER_MODE=regex bun run services/router-service/routerService.ts
# ... start all other Exercise 1 services
# Send: "what is 2 + 2" → mathApp responds
# Send: "weather in London" → weatherApp responds

# 3. Test ROUTER_MODE=llm — start the 3 new services
ROUTER_MODE=llm bun run services/router-service/routerService.ts
bun run services/guardrail-service/guardrailService.ts
bun run services/llm-router-service/llmRouterService.ts
bun run services/cot-math-service/cotMathService.ts
# Send: "What is the weather in Berlin?" → LLM classifies → weatherApp responds
# Send: "If I have 5 apples and buy 3 more" → cotMathService CoT → mathApp responds
# Send: "hack the government" → guardrailService blocks → guardrail-violation-events
```

---

## Critical Files

| File | Why critical |
|---|---|
| `shared/topics.ts` | All new services import from here; must exist before any service is written |
| `shared/types/events.ts` | `llm-router-service` needs `IntentMathEvent`, `IntentWeatherEvent`, etc. (already exist) plus new types |
| `services/router-service/routerService.ts` | Only modified existing service; regex path must stay identical |
| `services/llm-router-service/llmRouterService.ts` | The bridge — incorrect intent→topic mapping breaks the whole LLM pipeline |
| `infra/topics.sh` | Must be run before starting any new service |
