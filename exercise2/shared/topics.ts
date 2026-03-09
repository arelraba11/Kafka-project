// ─── Exercise 2 topics ───────────────────────────────────────────────────────
// These extend the Exercise 1 topic set with the LLM pipeline topics.

export const TOPICS = {
  // Inherited from Exercise 1 (re-exported for convenience)
  USER_INPUT:                  "user-input-events",
  USER_CONTROL:                "user-control-events",
  INTENT_MATH:                 "intent-math",
  INTENT_WEATHER:              "intent-weather",
  INTENT_EXCHANGE:             "intent-exchange",
  INTENT_CHAT:                 "intent-general-chat",
  APP_RESULTS:                 "app-results",
  BOT_RESPONSES:               "bot-responses",
  HISTORY_UPDATE:              "conversation-history-update",

  // Exercise 2 — LLM pipeline
  USER_INPUT_EVENTS:           "user_input_events",
  ROUTER_DECISION_EVENTS:      "router_decision_events",
  LLM_PROMPT_REQUESTS:         "llm_prompt_requests",
  LLM_RESPONSE_EVENTS:         "llm_response_events",
  FUNCTION_EXECUTION_REQUESTS: "function_execution_requests",
  BOT_OUTPUT_EVENTS:           "bot_output_events",
  GUARDRAIL_VIOLATION_EVENTS:  "guardrail_violation_events",
  COT_MATH_EXPRESSION_EVENTS:  "cot_math_expression_events",
} as const;

export type TopicName = typeof TOPICS[keyof typeof TOPICS];
