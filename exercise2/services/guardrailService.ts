// ─── guardrailService ────────────────────────────────────────────────────────
// Consumes:  user_input_events
// Produces:  guardrail_violation_events  (on violation)
//
// Detects unsafe topics (politics, malware) in user input.
// Safe messages are passed through — downstream services consume user_input_events directly.

import { createProducer, sendMessage } from "../kafka/kafka_producer";
import { createConsumer, subscribeAndRun, registerShutdown } from "../kafka/kafka_consumer";
import { TOPICS } from "../shared/topics";
import type { GuardrailViolationEvent, ViolationType } from "../shared/types/events";

// TODO: Implement guardrail detection logic
function detectViolation(input: string): ViolationType | null {
  // Stub — replace with real detection in implementation phase
  return null;
}

const producer = await createProducer();
const consumer = await createConsumer("guardrail-service");

registerShutdown([consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.USER_INPUT_EVENTS],
  async (_topic, key, value) => {
    const event = value as { userId: string; userInput: string; timestamp: string };
    const violation = detectViolation(event.userInput);

    if (violation) {
      const payload: GuardrailViolationEvent = {
        userId: event.userId,
        userInput: event.userInput,
        violationType: violation,
        message: "I cannot process this request due to safety protocols.",
        timestamp: new Date().toISOString(),
      };
      await sendMessage(producer, TOPICS.GUARDRAIL_VIOLATION_EVENTS, event.userId, payload);
      console.log(`[guardrail] Violation detected: userId=${event.userId} type=${violation}`);
    }
  }
);

console.log("[guardrail] GuardrailService started.");
