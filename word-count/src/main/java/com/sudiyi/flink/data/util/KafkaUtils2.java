package com.sudiyi.flink.data.util;

import com.alibaba.fastjson.JSON;
import com.sudiyi.flink.data.model.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author sdy
 */
public class KafkaUtils2 {

    private static final String BROKER_LIST = "172.32.2.95:9092";

    private static final String TOPIC = "flink-student";

    private static void writeToKafka() {

        Properties properties = new Properties();

        properties.put("bootstrap.servers", BROKER_LIST);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 100; i++) {
            Student student = new Student(i+1, "jbwang" + i+1, "password" + i+1, 10 + i);
            ProducerRecord record = new ProducerRecord<String, String>(TOPIC, null, null, JSON.toJSONString(student));
            producer.send(record);
            System.out.println("发送数据：" + JSON.toJSONString(student));
        }

        producer.flush();

    }

    public static void main(String[] args) {
        writeToKafka();
    }
}
