// ─── jsonParserService ────────────────────────────────────────────────────────
// Consumes:  llm_response_events
// Produces:  function_execution_requests  (on valid JSON)
//            error event to bot_output_events  (on invalid JSON)
//
// Safely parses the LLM's raw JSON response and routes it to the correct app.

import { createProducer, sendMessage } from "../kafka/kafka_producer";
import { createConsumer, subscribeAndRun, registerShutdown } from "../kafka/kafka_consumer";
import { TOPICS } from "../shared/topics";
import type {
  LLMResponseEvent,
  FunctionExecutionRequestEvent,
  BotOutputEvent,
} from "../shared/types/events";

function parseLLMResponse(raw: string): {
  intent: string;
  parameters: Record<string, unknown>;
  confidence: number;
} | null {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

const producer = await createProducer();
const consumer = await createConsumer("json-parser-service");

registerShutdown([consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.LLM_RESPONSE_EVENTS],
  async (_topic, _key, value) => {
    const event = value as LLMResponseEvent;
    const parsed = parseLLMResponse(event.rawResponse);

    if (!parsed) {
      console.error(`[json-parser] Invalid JSON from LLM for userId=${event.userId}`);
      const errorPayload: BotOutputEvent = {
        userId: event.userId,
        intent: event.intent,
        result: "Sorry, I could not process your request.",
        success: false,
        error: "LLM returned malformed JSON",
        timestamp: new Date().toISOString(),
      };
      await sendMessage(producer, TOPICS.BOT_OUTPUT_EVENTS, event.userId, errorPayload);
      return;
    }

    const payload: FunctionExecutionRequestEvent = {
      userId: event.userId,
      intent: event.intent,
      parameters: parsed.parameters,
      confidence: parsed.confidence,
      timestamp: new Date().toISOString(),
    };

    await sendMessage(producer, TOPICS.FUNCTION_EXECUTION_REQUESTS, event.userId, payload);
    console.log(`[json-parser] userId=${event.userId} intent=${event.intent} routed to apps`);
  }
);

console.log("[json-parser] JSONParserService started.");
