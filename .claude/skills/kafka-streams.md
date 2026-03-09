# Kafka Streams Skill

Kafka Streams processors should follow this pattern:

StreamsBuilder builder = new StreamsBuilder();

KStream<String, String> stream =
    builder.stream("wikimedia.recentchange");

stream
  .groupByKey()
  .count()
  .toStream()
  .to("output-topic");

KafkaStreams streams =
    new KafkaStreams(builder.build(), properties);

streams.start();