package com.sudiyi.flink.data.source;

import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

/**
 * @author sdy
 */
public class Main {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();

        properties.put("bootstrap.servers", "172.32.2.95:9092");
        properties.put("group.id", "metric-group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "earliest");

        DataStreamSource<String> dataStreamSource = environment.addSource(new FlinkKafkaConsumer011<>(
                "flink-metric",
                new SimpleStringSchema(),
                properties)).setParallelism(1);

        dataStreamSource.print();

        environment.execute("Flink add data source");
    }
}
