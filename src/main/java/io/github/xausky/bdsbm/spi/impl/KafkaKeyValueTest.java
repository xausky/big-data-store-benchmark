package io.github.xausky.bdsbm.spi.impl;

import io.github.xausky.bdsbm.spi.KeyValueTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by xausky on 10/20/16.
 */
public class KafkaKeyValueTest implements KeyValueTest {
    private KafkaConsumer<Integer,String> consumer;
    private KafkaProducer<Integer,String> producer;
    private int size = 0;
    public void init() throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Default");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "57e4beaf4710:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        producer = new KafkaProducer<Integer, String>(props);
        consumer = new KafkaConsumer<Integer,String>(props);
        consumer.subscribe(Collections.singletonList("test"));
    }

    public void store(List<String> data) throws Exception {
        size = data.size();
        for (String value:data){
            ProducerRecord<Integer,String> record = new ProducerRecord<Integer, String>("test",value.hashCode(),value);
            producer.send(record);
        }
        producer.close();
    }

    public int scan() throws Exception {
        int count = 0;
        while(count < size){
            ConsumerRecords<Integer, String> records = consumer.poll(100);
            for (ConsumerRecord<Integer,String> record : records){
                if (record.key() != record.value().hashCode()){
                    throw new Exception("hash code error");
                }
                count++;
            }
        }
        return count;
    }

    public void destroy() throws Exception {
        producer.close();
        consumer.close();
    }
}
