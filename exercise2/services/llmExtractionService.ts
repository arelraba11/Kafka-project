// ─── llmExtractionService ─────────────────────────────────────────────────────
// Consumes:  router_decision_events
// Produces:  llm_response_events
//
// Uses structured JSON output prompting to extract typed parameters from user input.

import { createProducer, sendMessage } from "../kafka/kafka_producer";
import { createConsumer, subscribeAndRun, registerShutdown } from "../kafka/kafka_consumer";
import { TOPICS } from "../shared/topics";
import { llmExtractionPrompt } from "../prompts/prompts";
import type { RouterDecisionEvent, LLMResponseEvent } from "../shared/types/events";

// TODO: Replace with real LLM call in implementation phase
async function callLLM(prompt: string): Promise<string> {
  throw new Error("LLM not yet integrated — implement callLLM()");
}

const producer = await createProducer();
const consumer = await createConsumer("llm-extraction-service");

registerShutdown([consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.ROUTER_DECISION_EVENTS],
  async (_topic, _key, value) => {
    const event = value as RouterDecisionEvent;

    const prompt = llmExtractionPrompt(event.intent, event.input);
    // TODO: call LLM and capture raw JSON string response
    const rawResponse = "{}"; // stub

    const payload: LLMResponseEvent = {
      userId: event.userId,
      intent: event.intent,
      rawResponse,
      timestamp: new Date().toISOString(),
    };

    await sendMessage(producer, TOPICS.LLM_RESPONSE_EVENTS, event.userId, payload);
    console.log(`[llm-extraction] userId=${event.userId} intent=${event.intent}`);
  }
);

console.log("[llm-extraction] LLMExtractionService started.");
