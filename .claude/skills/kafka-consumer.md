# Kafka Consumer Skill

Consumers should:

- subscribe to topics
- poll in a loop
- process records
- commit offsets

Typical pattern:

KafkaConsumer<String, String> consumer =
    new KafkaConsumer<>(properties);

consumer.subscribe(List.of("topic"));

while (true) {

  ConsumerRecords<String, String> records =
      consumer.poll(Duration.ofMillis(100));

  for (ConsumerRecord<String, String> record : records) {

    log.info("Key: {}", record.key());
    log.info("Value: {}", record.value());
    log.info("Partition: {}", record.partition());
    log.info("Offset: {}", record.offset());

  }

}