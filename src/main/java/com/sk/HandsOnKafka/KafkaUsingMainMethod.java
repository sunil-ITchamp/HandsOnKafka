package com.sk.HandsOnKafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaUsingMainMethod {
    public static void main(String[] args) {
        ExecutorService service = Executors.newFixedThreadPool(2);
        try
            (Consumer<String, String>  consumer = new KafkaConsumer<String, String>(consumerProperties());
             Producer<String, String> producer = new KafkaProducer<String, String>(producerProperties())) {
            service.execute(new Runnable() {
                @Override
               public void run() {
                    System.out.println(Thread.currentThread().getName());
                    for (int i = 1; i <= 10; i++) {
                        producer.send(new ProducerRecord<String, String>("my-topic", ("key" + i), ("value" + i)));
                    }
                }
                });

            service.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println("Executing consumer thread");
                    consumer.subscribe(Collections.singletonList("sk_topic1"));
                    while (true) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                        for (ConsumerRecord<String, String> record : records) {
                            System.out.println(record.value());
                        }
                    }
                }
            });
        }
        }

public static Properties producerProperties(){
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return props;
}

public static Properties consumerProperties(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "sk_topic1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
}
}