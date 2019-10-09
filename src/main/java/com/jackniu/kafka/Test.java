package com.jackniu.kafka;


import com.jackniu.kafka.clients.producer.KafkaProducer;
import com.jackniu.kafka.clients.producer.ProducerConfig;
import com.jackniu.kafka.clients.producer.ProducerRecord;
import com.jackniu.kafka.clients.producer.RecordMetadata;
import com.jackniu.kafka.common.serialization.StringSerializer;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Test {
    public static void main(String[] args) {
        System.out.println("测试Kafka环境是否正常");

        Properties properties = new Properties();
        List<String> ll = new ArrayList<String>();
        ll.add("127.0.0.1:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,ll);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        System.out.println(properties);
        KafkaProducer producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> msg = new ProducerRecord<String, String>("test", "kafkaSourceAnalysis Test");
        try {
            // 发送失败
            RecordMetadata metadata=(RecordMetadata) producer.send(msg, (recordMetadata, e) -> System.out.println("send success")).get();
            System.out.println(metadata.offset()+"  "+ metadata.topic() +"  "+ metadata.partition() +" "+ metadata.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
