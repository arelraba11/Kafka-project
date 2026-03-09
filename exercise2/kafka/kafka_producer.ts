import { Kafka, Producer } from "kafkajs";

// ─── Shared Kafka instance for Exercise 2 ────────────────────────────────────

const kafka = new Kafka({
  clientId: "llm-bot-ex2",
  brokers: ["localhost:9092"],
});

export async function createProducer(): Promise<Producer> {
  const producer = kafka.producer();
  await producer.connect();
  return producer;
}

export async function sendMessage(
  producer: Producer,
  topic: string,
  key: string,
  value: unknown
): Promise<void> {
  await producer.send({
    topic,
    messages: [
      {
        key,
        value: JSON.stringify(value),
      },
    ],
  });
}
