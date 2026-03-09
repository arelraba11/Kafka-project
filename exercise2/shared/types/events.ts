// ─── Exercise 2 event types ──────────────────────────────────────────────────
// Extends Exercise 1 event types with LLM pipeline events.

export type LLMIntent =
  | "getWeather"
  | "calculateMath"
  | "currencyExchange"
  | "generalChat";

// ─── guardrail_violation_events ───────────────────────────────────────────────
// Producer: guardrailService.ts
// Consumer: responseAggregator / userInterface

export type ViolationType = "politics" | "malware";

export interface GuardrailViolationEvent {
  userId: string;
  userInput: string;
  violationType: ViolationType;
  message: string;
  timestamp: string;
}

// ─── router_decision_events ───────────────────────────────────────────────────
// Producer: llmRouterService.ts
// Consumers: llmExtractionService.ts, cotMathService.ts

export interface RouterDecisionEvent {
  userId: string;
  input: string;
  intent: LLMIntent;
  parameters: Record<string, unknown>;
  confidence: number;
  timestamp: string;
}

// ─── llm_prompt_requests ─────────────────────────────────────────────────────
// Producer: llmExtractionService.ts
// Consumer: (LLM gateway — future)

export interface LLMPromptRequestEvent {
  userId: string;
  intent: LLMIntent;
  prompt: string;
  timestamp: string;
}

// ─── llm_response_events ─────────────────────────────────────────────────────
// Producer: llmExtractionService.ts
// Consumer: jsonParserService.ts

export interface LLMResponseEvent {
  userId: string;
  intent: LLMIntent;
  rawResponse: string;
  timestamp: string;
}

// ─── function_execution_requests ─────────────────────────────────────────────
// Producer: jsonParserService.ts
// Consumers: mathApp, weatherApp, exchangeApp, generalChatApp

export interface FunctionExecutionRequestEvent {
  userId: string;
  intent: LLMIntent;
  parameters: Record<string, unknown>;
  confidence: number;
  timestamp: string;
}

// ─── bot_output_events ───────────────────────────────────────────────────────
// Producer: apps (mathApp, weatherApp, exchangeApp, generalChatApp)
// Consumer: responseAggregator

export interface BotOutputEvent {
  userId: string;
  intent: LLMIntent;
  result: string;
  success: boolean;
  error?: string;
  timestamp: string;
}

// ─── cot_math_expression_events ──────────────────────────────────────────────
// Producer: cotMathService.ts
// Consumer: mathApp

export interface CotMathExpressionEvent {
  userId: string;
  originalInput: string;
  expression: string;
  reasoning: string;
  timestamp: string;
}
