# Coding Conventions

Language: Java

Logging:
Use SLF4J.

Kafka:

Use org.apache.kafka.clients library.

Serialization:

Use StringSerializer and StringDeserializer unless required otherwise.

Always log Kafka metadata:

topic
partition
offset