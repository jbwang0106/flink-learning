package com.sudiyi.flink.data.sink;

import com.alibaba.fastjson.JSON;
import com.sudiyi.flink.data.model.Student;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * @author sdy
 */
public class SinkMySqlMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.32.2.95:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "student-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        environment.addSource(new FlinkKafkaConsumer011<>("flink-student", new SimpleStringSchema(), properties))
                .setParallelism(1)
                .map(value -> JSON.parseObject(value, Student.class))
                .addSink(new SinkToMySql());

        environment.execute("Flink add sink");
    }
}
