package com.sudiyi.flink.util;

import com.alibaba.fastjson.JSON;
import com.sudiyi.flink.model.Metric;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaUtils {

    private static final String brokerList = "124.65.224.66:9092";

    private static final String topic = "metric";

    public static void writeToKafka() throws InterruptedException {

        Properties properties = new Properties();

        properties.put("bootstrap.servers", brokerList);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Metric metric = new Metric();

        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("mem");

        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        tags.put("cluster", "zhisheng");
        tags.put("host_ip", "101.147.022.106");

        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        metric.setTags(tags);
        metric.setFields(fields);

        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(metric));

        producer.send(record);
        System.out.println("发送数据：" + JSON.toJSONString(metric));

        producer.flush();

    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(300);
            writeToKafka();
        }
    }
}
