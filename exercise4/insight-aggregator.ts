/**
 * insight-aggregator.ts
 *
 * Consumes analysis-sentiment and analysis-urgency topics concurrently.
 * Correlates results by message id using an in-memory Map.
 * Emits a strong alert or normal log once both branches arrive.
 *
 * Usage:
 *   bun run insight-aggregator.ts
 *
 * Topics:
 *   IN ← analysis-sentiment   (group: insight-aggregator-group)
 *   IN ← analysis-urgency     (group: insight-aggregator-group)
 *
 * Business logic:
 *   sentiment === "NEGATIVE" AND urgency === "Urgent"
 *     → 🚨 STRONG ALERT
 *   otherwise
 *     → [insight] normal log
 */

import { kafka } from "./kafka/kafkaClient.ts";
import { TOPICS } from "./shared/topics.ts";

// ---------------------------------------------------------------------------
// Local types matching the actual wire schemas from the Python workers
// ---------------------------------------------------------------------------

interface SentimentMessage {
  id: string;
  sentiment: "POSITIVE" | "NEGATIVE";
  score: number;
}

interface UrgencyMessage {
  id: string;
  urgency: "Urgent" | "Complaint" | "General Inquiry";
  score: number;
}

interface PartialInsight {
  sentiment?: Pick<SentimentMessage, "sentiment" | "score">;
  urgency?: Pick<UrgencyMessage, "urgency" | "score">;
}

// ---------------------------------------------------------------------------
// Shared in-memory correlation store
// ---------------------------------------------------------------------------

const store = new Map<string, PartialInsight>();

const GROUP_ID = "insight-aggregator-group";

// ---------------------------------------------------------------------------
// Business logic
// ---------------------------------------------------------------------------

function evaluateInsight(id: string, insight: Required<PartialInsight>): void {
  const { sentiment, urgency } = insight;

  if (sentiment.sentiment === "NEGATIVE" && urgency.urgency === "Urgent") {
    console.log(`\n🚨 STRONG ALERT: customer issue requires immediate attention`);
    console.log(
      `   id=${id}  sentiment=${sentiment.sentiment} (${sentiment.score})  urgency=${urgency.urgency} (${urgency.score})\n`
    );
  } else {
    console.log(
      `[insight] id=${id} sentiment=${sentiment.sentiment} urgency=${urgency.urgency}`
    );
  }
}

function upsertAndEvaluate(id: string, update: Partial<PartialInsight>): void {
  const existing = store.get(id) ?? {};
  const merged: PartialInsight = { ...existing, ...update };

  if (merged.sentiment && merged.urgency) {
    evaluateInsight(id, merged as Required<PartialInsight>);
    store.delete(id);
  } else {
    store.set(id, merged);
  }
}

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

async function runConsumer(): Promise<void> {
  const consumer = kafka.consumer({ groupId: GROUP_ID });
  await consumer.connect();

  await consumer.subscribe({ topic: TOPICS.ANALYSIS_SENTIMENT, fromBeginning: false });
  await consumer.subscribe({ topic: TOPICS.ANALYSIS_URGENCY, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      if (topic === TOPICS.ANALYSIS_SENTIMENT) {
        const msg = JSON.parse(raw) as SentimentMessage;
        console.log(`[insight] received sentiment ${msg.id}`);
        upsertAndEvaluate(msg.id, {
          sentiment: { sentiment: msg.sentiment, score: msg.score },
        });
      } else if (topic === TOPICS.ANALYSIS_URGENCY) {
        const msg = JSON.parse(raw) as UrgencyMessage;
        console.log(`[insight] received urgency ${msg.id}`);
        upsertAndEvaluate(msg.id, {
          urgency: { urgency: msg.urgency, score: msg.score },
        });
      }
    },
  });
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("[insight] Connecting consumer...");

  await runConsumer();
}

main().catch((err) => {
  console.error("[insight] Fatal error:", err);
  process.exit(1);
});
