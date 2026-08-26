package com.sb.stock.trade.kafka.service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

	private static final String TOPIC_1 = "my_topic_1";
	private static final String TOPIC_2 = "my_topic_2";
	private static final String TOPIC_3 = "my_topic_3";

	private static final String TOPIC_NEW = "my_new_topic_1";
	private final KafkaTemplate<String, String> kafkaTemplate;

	public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	public void sendMessage(String message) {
		CompletableFuture<SendResult<String, String>> map1 = kafkaTemplate.send(TOPIC_1, message + TOPIC_1);
		CompletableFuture<SendResult<String, String>> map2 = kafkaTemplate.send(TOPIC_2, message + TOPIC_2);
		CompletableFuture<SendResult<String, String>> map3 = kafkaTemplate.send(TOPIC_3, message + TOPIC_3);

	//	CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);

		try {

			System.out.println("Return map1 " + map1.get().getProducerRecord());
			System.out.println("Return map2  " + map2.get().getProducerRecord());
			System.out.println("Return map3  " + map3.get().getProducerRecord());

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Message sent : " + message);
	}

	public void sendNewMessage(String message) {

		ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NEW, message);

		SendResult<String, String> abvc;
		try {
			abvc = kafkaTemplate.send(record).get();

			RecordMetadata metadata = abvc.getRecordMetadata();

			System.out.println("metadata" +metadata);

			System.out.println("Message sent to topic " + metadata.topic() + " at offset " + metadata.offset()
					+ " TimeStamp " + metadata.timestamp());

		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ExecutionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		System.out.println("Message sent : " + message);
	}
}