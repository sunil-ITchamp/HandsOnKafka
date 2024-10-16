package com.sk.HandsOnKafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumer1 {

    private static int count1 = 0;
    private static int count2 = 0;
    private static int count3 = 0;

    @KafkaListener(topics = "toptest1", groupId = "sk-group1")
    public void consumer1(String message){
        count1++;
        log.info("Consumer-1 : " + message + " Count = " + count1);
    }

    @KafkaListener(topics = "toptest1", groupId = "sk-group1")
    public void consumer2(String message){
        count2++;
        log.info("Consumer-2 : " + message + " Count = " + count2);
    }

    @KafkaListener(topics = "toptest1", groupId = "sk-group1")
    public void consumer3(String message){
        count3++;
        log.info("Consumer-3 : " + message + " Count = " + count3);
    }
}
