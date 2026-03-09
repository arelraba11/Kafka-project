import { Kafka, type KafkaConfig } from "kafkajs";

const BROKER = process.env.KAFKA_BROKER ?? "127.0.0.1:9092";
const CLIENT_ID = process.env.KAFKA_CLIENT_ID ?? "exercise4";

const config: KafkaConfig = {
  clientId: CLIENT_ID,
  brokers: [BROKER],
};

// Shared Kafka instance — import this in every service instead of
// constructing a new Kafka() object per file.
export const kafka = new Kafka(config);
