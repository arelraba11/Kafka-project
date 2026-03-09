# Kafka Pipeline Architecture

This project implements a Kafka-based data engineering pipeline.

Data Flow:

Wikimedia SSE API
→ WikimediaChangesProducer
→ Kafka topic: wikimedia.recentchange
→ Kafka Streams processors
→ Aggregation topics
→ OpenSearchConsumer
→ OpenSearch index

Modules:

kafka-basics
Contains examples of Kafka producers and consumers.

kafka-producer-wikimedia
Streams live events from Wikimedia Recent Changes API.

kafka-streams-wikimedia
Kafka Streams application performing analytics.

kafka-consumer-opensearch
Consumes events from Kafka and indexes them into OpenSearch.

Goal:
Extend this architecture with additional data engineering exercises.