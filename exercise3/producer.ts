// producer.ts — CLI fire-and-forget review producer
//
// Usage:
//   bun run producer.ts
//   Type a review and press Enter. Ctrl+C to exit.
//
// Each line of stdin is wrapped in a ReviewEvent and sent to raw-reviews-topic.

import { createProducer } from "./kafka/kafkaClient";
import { TOPICS } from "./shared/topics";
import type { ReviewEvent } from "./shared/types";

const CLIENT_ID = process.env.REVIEW_PRODUCER_CLIENT_ID ?? "review-producer";

async function main(): Promise<void> {
  const producer = await createProducer(CLIENT_ID);

  console.log("[producer] Connected. Type a review and press Enter. Ctrl+C to exit.\n");
  process.stdout.write("> ");

  const decoder = new TextDecoder();

  for await (const chunk of Bun.stdin.stream()) {
    const text = decoder.decode(chunk).trim();
    if (!text) continue;

    const reviewId = crypto.randomUUID();

    const event: ReviewEvent = {
      reviewId,
      text,
      timestamp: new Date().toISOString(),
    };

    // Fire-and-forget: send without awaiting acks
    producer.send({
      topic: TOPICS.RAW_REVIEWS,
      messages: [{ key: reviewId, value: JSON.stringify(event) }],
    });

    console.log(`[producer] Sent review ${reviewId}`);
    process.stdout.write("> ");
  }

  await producer.disconnect();
}

main();
