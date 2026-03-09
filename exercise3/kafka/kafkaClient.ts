import { Kafka, type Producer, type Consumer } from "kafkajs";

const BROKER = process.env.KAFKA_BROKER ?? "localhost:9092";

function createKafka(clientId: string): Kafka {
  return new Kafka({
    clientId,
    brokers: [BROKER],
  });
}

export async function createProducer(clientId: string): Promise<Producer> {
  const kafka = createKafka(clientId);
  const producer = kafka.producer();
  await producer.connect();
  return producer;
}

export async function createConsumer(
  groupId: string,
  clientId: string
): Promise<Consumer> {
  const kafka = createKafka(clientId);
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  return consumer;
}
