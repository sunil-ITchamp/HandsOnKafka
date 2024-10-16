package com.sk.HandsOnKafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@SpringBootApplication
@RestController
@Slf4j
public class HandsOnKafkaApplication {

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	private static final String TOPICNAME = "toptest1";

	public static void main(String[] args) {
		SpringApplication.run(HandsOnKafkaApplication.class, args);
	}

	@PostMapping("/api/messages/")
	public String sendMessageToTopic(@RequestBody String message){
		log.info("log.info" + message);
		//kafkaTemplate.send(topicName, message);
		for (int i =1; i<10000; i++) {
			kafkaTemplate.send(TOPICNAME, UUID.randomUUID().toString(), message);
		}
		return "success";
	}

//	@KafkaListener (topics = "topic222", groupId = "sktest1")
//	public void listenMessage1(String message){
//		System.out.println ("listenMessage1 Consumer : " + message);
//		log.info("listenMessage1 Consumer : " + message);
//	}

//	@KafkaListener (topics = "topic222", groupId = "sktest12")
//	public void listenMessage2(String message){
//		System.out.println ("listenMessage2 Consumer : " + message);
//	}
}
