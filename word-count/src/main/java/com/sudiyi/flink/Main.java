package com.sudiyi.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @author sdy
 */
public class Main {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();

        properties.put("bootstrap.servers", "124.65.224.66:9092");
        properties.put("zookeeper.connect", "172.32.2.12:2182");
        properties.put("group.id", "metric-group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = environment.addSource(new FlinkKafkaConsumer010<>(
                "metric",
                new SimpleStringSchema(),
                properties)).setParallelism(1);

        dataStreamSource.print();

        environment.execute("Flink add data source");
    }
}
