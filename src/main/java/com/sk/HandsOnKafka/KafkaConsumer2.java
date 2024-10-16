package com.sk.HandsOnKafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumer2 {

//    @KafkaListener(topics = "toptest1", groupId = "sk-group1")
//    public void consumer2(String message){
//        log.info("Consumer-2 : " + message);
//    }
}
