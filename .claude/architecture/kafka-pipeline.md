# Kafka Pipeline Architecture

This repository implements a Kafka-based data pipeline.

Data flow:

Wikimedia SSE API
→ WikimediaChangesProducer
→ Kafka topic: wikimedia.recentchange
→ Kafka Streams processors
→ Aggregation topics
→ OpenSearchConsumer
→ OpenSearch index

Modules:

kafka-basics
Basic producer and consumer examples.

kafka-producer-wikimedia
Streams events from Wikimedia Recent Changes API into Kafka.

kafka-streams-wikimedia
Kafka Streams application performing aggregations.

kafka-consumer-opensearch
Consumes events and indexes them into OpenSearch.

Goal:
Extend this pipeline with additional data engineering exercises.