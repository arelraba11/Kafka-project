/**
 * producer.ts
 *
 * Reads raw customer messages from CLI and publishes them to
 * the raw-customer-messages Kafka topic.
 *
 * Usage:
 *   bun run producer.ts                          — interactive mode
 *   bun run producer.ts "Jane called 555-1234"   — single message, then exit
 *
 * Topics:
 *   OUT → raw-customer-messages
 */

import { kafka } from "./kafka/kafkaClient.ts";
import { TOPICS } from "./shared/topics.ts";
import type { RawMessage } from "./shared/types.ts";

async function sendMessage(
  producer: ReturnType<typeof kafka.producer>,
  text: string
): Promise<void> {
  const message_id = crypto.randomUUID();

  const payload: RawMessage = {
    message_id,
    text,
    timestamp: new Date().toISOString(),
    source: "cli",
  };

  await producer.send({
    topic: TOPICS.RAW_CUSTOMER_MESSAGES,
    messages: [{ key: message_id, value: JSON.stringify(payload) }],
  });

  console.log(`[producer] Sent message ${message_id}`);
}

async function main(): Promise<void> {
  const producer = kafka.producer();
  await producer.connect();

  const argText = process.argv[2]?.trim();

  if (argText) {
    // Single-shot mode: one argument → send once → exit
    await sendMessage(producer, argText);
  } else {
    // Interactive mode: read lines from stdin until Ctrl+C
    console.log("[producer] Connected. Type a message and press Enter. Ctrl+C to exit.\n");
    process.stdout.write("> ");

    const decoder = new TextDecoder();

    for await (const chunk of Bun.stdin.stream()) {
      const text = decoder.decode(chunk).trim();
      if (!text) {
        process.stdout.write("> ");
        continue;
      }

      await sendMessage(producer, text);
      process.stdout.write("> ");
    }
  }

  await producer.disconnect();
}

main().catch((err) => {
  console.error("[producer] Fatal error:", err);
  process.exit(1);
});
