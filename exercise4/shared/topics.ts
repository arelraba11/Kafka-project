// Kafka topic names for the customer support analysis pipeline

export const TOPICS = {
  RAW_CUSTOMER_MESSAGES: "raw-customer-messages",
  SANITIZED_MESSAGES: "sanitized-messages",
  ANALYSIS_SENTIMENT: "analysis-sentiment",
  ANALYSIS_URGENCY: "analysis-urgency",
} as const;

export type TopicName = (typeof TOPICS)[keyof typeof TOPICS];

// Consumer group IDs
export const CONSUMER_GROUPS = {
  SANITIZER: "sanitizer-group",
  SENTIMENT: "sentiment-group",
  URGENCY: "urgency-group",
  AGGREGATOR: "aggregator-group",
} as const;
