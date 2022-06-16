package org.apache.flink.streaming.examples.join;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;

public class TopicOutputSchema implements KafkaRecordSerializationSchema<Tuple2<String, Integer>> {

    final String TOPIC;

    public TopicOutputSchema(final String TOPIC) {
        this.TOPIC = TOPIC;
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
            throws Exception {
        KafkaRecordSerializationSchema.super.open(context, sinkContext);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            Tuple2<String, Integer> output, KafkaSinkContext kafkaSinkContext, Long timestamp) {
        String value = "";
        value += output.f0;
        value += "," + output.f1;
        return new ProducerRecord<>(TOPIC, "out".getBytes(), value.getBytes());
    }
}
