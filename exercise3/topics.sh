#!/usr/bin/env bash
# Creates the Kafka topics required for Exercise 3.
# Run this after `docker compose up -d` and Kafka is healthy.

BROKER="${KAFKA_BROKER:-localhost:9092}"

kafka-topics.sh --create \
  --topic raw-reviews-topic \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server "$BROKER"

kafka-topics.sh --create \
  --topic processed-insights-topic \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server "$BROKER"

echo "Topics created."
