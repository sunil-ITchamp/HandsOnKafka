package com.sk.HandsOnKafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.UUID;

@Service
@Slf4j
public class KafkaPublisher {
    private static final String TOPICNAME = "toptest1";

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    public String sendMessage(String message){
        log.info("log.info" + message);
        //kafkaTemplate.send(topicName, message);
        for (int i =1; i<10000; i++) {
            kafkaTemplate.send(TOPICNAME, UUID.randomUUID().toString(), message);
        }
        log.info("+++++++++++++++ Successfuly psoted messages");
        return "success";
    }
}
