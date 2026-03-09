# Kafka Consumer Pattern

Kafka consumers should:

subscribe to topics

poll records in a loop

process each record

commit offsets if needed

Example pattern:

KafkaConsumer<String, String> consumer =
new KafkaConsumer<>(properties);

consumer.subscribe(List.of("topic"));

while (true) {

ConsumerRecords<String, String> records =
consumer.poll(Duration.ofMillis(100));

for (ConsumerRecord<String, String> record : records) {

log.info("Key {}", record.key());
log.info("Value {}", record.value());
log.info("Partition {}", record.partition());
log.info("Offset {}", record.offset());

}

}