# Kafka Producer Pattern

Typical producer configuration:

Properties properties = new Properties();

properties.setProperty(
ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
"127.0.0.1:9092"
);

properties.setProperty(
ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
StringSerializer.class.getName()
);

properties.setProperty(
ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
StringSerializer.class.getName()
);

KafkaProducer<String, String> producer =
new KafkaProducer<>(properties);

Always log metadata:

topic
partition
offset