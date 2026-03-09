// ─── llmRouterService ────────────────────────────────────────────────────────
// Consumes:  user_input_events
// Produces:  router_decision_events
//
// Uses Few-Shot prompting to classify user intent into:
//   getWeather | calculateMath | currencyExchange | generalChat

import { createProducer, sendMessage } from "../kafka/kafka_producer";
import { createConsumer, subscribeAndRun, registerShutdown } from "../kafka/kafka_consumer";
import { TOPICS } from "../shared/topics";
import { llmRouterPrompt } from "../prompts/prompts";
import type { RouterDecisionEvent, LLMIntent } from "../shared/types/events";

// ─── LLM stub ────────────────────────────────────────────────────────────────
// TODO: Replace with real LLM call in implementation phase.

async function callLLM(_prompt: string): Promise<string> {
  throw new Error("LLM not yet integrated — implement callLLM()");
}

// ─── Regex fallback classifier ────────────────────────────────────────────────
// Used while the LLM is not yet integrated.
// Maps user input to Ex2 intent names and extracts basic parameters.

const MATH_RE     = /[\d]+\s*[+\-*/]\s*[\d]+|(?:calculate|compute|what is)\s+[\d]/i;
const WEATHER_RE  = /\b(weather|temperature|forecast|rain|sunny|hot|cold)\b/i;
const CURRENCY_RE = /\b(USD|EUR|ILS|GBP|JPY|CHF|CAD|AUD|convert|exchange)\b/i;

function regexClassify(input: string): { intent: LLMIntent; parameters: Record<string, unknown>; confidence: number } {
  if (MATH_RE.test(input)) {
    return { intent: "calculateMath", parameters: { expression: input }, confidence: 0.75 };
  }
  if (WEATHER_RE.test(input)) {
    const cityMatch = input.match(/\bin\s+([A-Za-z\s]+?)(?:\?|$)/i);
    return { intent: "getWeather", parameters: { city: cityMatch?.[1]?.trim() ?? "unknown" }, confidence: 0.75 };
  }
  if (CURRENCY_RE.test(input)) {
    const codes = input.match(/\b(USD|EUR|ILS|GBP|JPY|CHF|CAD|AUD)\b/gi) ?? [];
    return { intent: "currencyExchange", parameters: { from: codes[0] ?? "USD", to: codes[1] ?? "ILS" }, confidence: 0.75 };
  }
  return { intent: "generalChat", parameters: {}, confidence: 0.70 };
}

// ─── Intent classification ────────────────────────────────────────────────────
// Tries the LLM first; falls back to regex if the LLM is not available.

async function classifyIntent(
  userInput: string
): Promise<{ intent: LLMIntent; parameters: Record<string, unknown>; confidence: number }> {
  try {
    const prompt = llmRouterPrompt(userInput);
    const raw = await callLLM(prompt);
    const parsed = JSON.parse(raw) as { intent: LLMIntent; parameters: Record<string, unknown>; confidence: number };
    console.log(`[llm-router] LLM classification: intent=${parsed.intent} confidence=${parsed.confidence}`);
    return parsed;
  } catch (err) {
    console.warn(`[llm-router] LLM unavailable, using regex fallback. Reason: ${(err as Error).message}`);
    return regexClassify(userInput);
  }
}

// ─── Main ────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("llm-router-service");

registerShutdown([consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.USER_INPUT_EVENTS],
  async (_topic, _key, value) => {
    const event = value as { userId: string; userInput: string; timestamp: string };

    console.log(`[llm-router] Message received — userId=${event.userId} input="${event.userInput}"`);

    const { intent, parameters, confidence } = await classifyIntent(event.userInput);

    console.log(`[llm-router] Detected intent="${intent}" confidence=${confidence} parameters=${JSON.stringify(parameters)}`);

    const payload: RouterDecisionEvent = {
      userId: event.userId,
      input: event.userInput,
      intent,
      parameters,
      confidence,
      timestamp: new Date().toISOString(),
    };

    await sendMessage(producer, TOPICS.ROUTER_DECISION_EVENTS, event.userId, payload);

    console.log(`[llm-router] Published to router_decision_events — userId=${event.userId} intent=${intent}`);
  }
);

console.log("[llm-router] LLMRouterService started.");
