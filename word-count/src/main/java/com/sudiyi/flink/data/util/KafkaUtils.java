package com.sudiyi.flink.data.util;

import com.alibaba.fastjson.JSON;
import com.sudiyi.flink.data.model.Metric;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author sdy
 */
public class KafkaUtils {

    private static final String BROKER_LIST = "172.32.2.95:9092";

    private static final String TOPIC = "flink-metric";

    private static void writeToKafka() {

        Properties properties = new Properties();

        properties.put("bootstrap.servers", BROKER_LIST);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Metric metric = new Metric();

        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("mem");

        Map<String, String> tags = new HashMap<>(2);
        Map<String, Object> fields = new HashMap<>(4);
        tags.put("cluster", "jbwang0106");
        tags.put("host_ip", "172.32.2.96");

        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        metric.setTags(tags);
        metric.setFields(fields);

        ProducerRecord record = new ProducerRecord<String, String>(TOPIC, null, null, JSON.toJSONString(metric));

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
