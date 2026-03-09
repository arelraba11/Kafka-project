/**
 * sanitizer.ts
 *
 * Consumes raw customer messages, calls Ollama to scrub PII,
 * and publishes sanitized messages downstream.
 *
 * Usage:
 *   bun run sanitizer.ts
 *
 * Topics:
 *   IN  ← raw-customer-messages  (group: sanitizer-group)
 *   OUT → sanitized-messages
 *
 * Dependencies:
 *   Ollama must be running locally (http://localhost:11434)
 *   Model: llama3 or mistral
 */

import { kafka } from "./kafka/kafkaClient.ts";
import { TOPICS, CONSUMER_GROUPS } from "./shared/topics.ts";
import type { RawMessage } from "./shared/types.ts";

const OLLAMA_BASE_URL = process.env.OLLAMA_BASE_URL ?? "http://localhost:11434";
const OLLAMA_MODEL = process.env.OLLAMA_MODEL ?? "llama3";

// Instructs the model to return only the scrubbed text, nothing else.
function buildSanitizePrompt(text: string): string {
  return [
    "You are a PII scrubbing assistant.",
    "Replace every person's name with [NAME].",
    "Replace every phone number (any format) with [NUMBER].",
    "Return only the sanitized text. Do not add explanations, notes, or extra lines.",
    "",
    `Input: ${text}`,
    "Output:",
  ].join("\n");
}

async function callOllama(text: string): Promise<string> {
  const response = await fetch(`${OLLAMA_BASE_URL}/api/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: OLLAMA_MODEL,
      prompt: buildSanitizePrompt(text),
      stream: false,
    }),
  });

  if (!response.ok) {
    throw new Error(`Ollama error: ${response.status} ${response.statusText}`);
  }

  const data = (await response.json()) as { response: string };
  return data.response.trim();
}

async function main(): Promise<void> {
  const consumer = kafka.consumer({ groupId: CONSUMER_GROUPS.SANITIZER });
  const producer = kafka.producer();

  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({
    topic: TOPICS.RAW_CUSTOMER_MESSAGES,
    fromBeginning: false,
  });

  console.log("[sanitizer] Connected. Waiting for messages...");

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      const parsed = JSON.parse(raw) as RawMessage;
      const { message_id, text, timestamp } = parsed;

      console.log(`[sanitizer] received ${message_id}`);

      const sanitizedText = await callOllama(text);

      await producer.send({
        topic: TOPICS.SANITIZED_MESSAGES,
        messages: [
          {
            key: message_id,
            value: JSON.stringify({
              id: message_id,
              text: sanitizedText,
              timestamp,
            }),
          },
        ],
      });

      console.log(`[sanitizer] sanitized ${message_id}`);

      await consumer.commitOffsets([
        { topic, partition, offset: (Number(message.offset) + 1).toString() },
      ]);
    },
  });
}

main().catch((err) => {
  console.error("[sanitizer] Fatal error:", err);
  process.exit(1);
});
