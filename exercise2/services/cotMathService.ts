// ─── cotMathService ───────────────────────────────────────────────────────────
// Consumes:  router_decision_events  (only when intent = calculateMath)
// Produces:  cot_math_expression_events
//
// Uses Chain-of-Thought prompting to convert a natural language word problem
// into an arithmetic expression, then forwards it to the mathApp.

import { createProducer, sendMessage } from "../kafka/kafka_producer";
import { createConsumer, subscribeAndRun, registerShutdown } from "../kafka/kafka_consumer";
import { TOPICS } from "../shared/topics";
import { cotMathPrompt } from "../prompts/prompts";
import type { RouterDecisionEvent, CotMathExpressionEvent } from "../shared/types/events";

// TODO: Replace with real LLM call in implementation phase
async function callLLM(prompt: string): Promise<string> {
  throw new Error("LLM not yet integrated — implement callLLM()");
}

function isWordProblem(input: string): boolean {
  // A "word problem" has letters and words rather than a direct expression
  // TODO: refine heuristic in implementation phase
  return /[a-zA-Z]/.test(input) && !/^\s*[\d\s+\-*/().]+\s*$/.test(input);
}

const producer = await createProducer();
const consumer = await createConsumer("cot-math-service");

registerShutdown([consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.ROUTER_DECISION_EVENTS],
  async (_topic, _key, value) => {
    const event = value as RouterDecisionEvent;

    if (event.intent !== "calculateMath") return;
    if (!isWordProblem(event.input)) return;

    const prompt = cotMathPrompt(event.input);
    // TODO: call LLM, parse { reasoning, expression } from JSON response
    const reasoning = ""; // stub
    const expression = ""; // stub

    const payload: CotMathExpressionEvent = {
      userId: event.userId,
      originalInput: event.input,
      expression,
      reasoning,
      timestamp: new Date().toISOString(),
    };

    await sendMessage(producer, TOPICS.COT_MATH_EXPRESSION_EVENTS, event.userId, payload);
    console.log(`[cot-math] userId=${event.userId} expression="${expression}"`);
  }
);

console.log("[cot-math] CotMathService started.");
