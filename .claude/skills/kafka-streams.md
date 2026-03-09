# Kafka Streams Pattern

Kafka Streams processing follows this pattern:

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