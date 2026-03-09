#!/usr/bin/env bash
# Creates all Exercise 2 Kafka topics.
# Run after Kafka is up: bash topics.sh

KAFKA_CONTAINER="kafka"
PARTITIONS=1
REPLICATION=1

TOPICS=(
  "user_input_events"
  "router_decision_events"
  "llm_prompt_requests"
  "llm_response_events"
  "function_execution_requests"
  "bot_output_events"
  "guardrail_violation_events"
  "cot_math_expression_events"
)

for TOPIC in "${TOPICS[@]}"; do
  echo "Creating topic: $TOPIC"
  docker exec "$KAFKA_CONTAINER" kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION"
done

echo "All topics created."
