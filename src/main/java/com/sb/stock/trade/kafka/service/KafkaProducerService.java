package com.sb.stock.trade.kafka.service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sb.stock.trade.service.impl.StockServiceImpl;
import com.sb.stock.trade.view.PlaceOrderVo;

@Service
public class KafkaProducerService {

	private static final Logger logger = LoggerFactory.getLogger(StockServiceImpl.class);

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

		// CompletableFuture<SendResult<String, String>> future =
		// kafkaTemplate.send(topic, message);

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

	
	public void sendMessage(String topic, String message) {
		/*
		 * 
		 * CompletableFuture<SendResult<String, String>> map1 =
		 * kafkaTemplate.send(topic, message + topic);
		 * 
		 * // CompletableFuture<SendResult<String, String>> future = //
		 * kafkaTemplate.send(topic, message);
		 * 
		 * try {
		 * 
		 * System.out.println("Return map1 " + map1.get().getProducerRecord());
		 * 
		 * 
		 * } catch (InterruptedException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } catch (ExecutionException e) { // TODO Auto-generated
		 * catch block e.printStackTrace(); }
		 * 
		 * System.out.println("Message sent : " + message);
		 */}
	
	
	public void sendOrderMessage(PlaceOrderVo placeOrderVo) {

		String jsonPayload = null;
		try {
			jsonPayload = new ObjectMapper().writeValueAsString(placeOrderVo);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(placeOrderVo.getTicker(),
				jsonPayload);

		
    //    kafkaTemplate.send(placeOrderVo.getTicker(), placeOrderVo.getTicker(), placeOrderVo);

		future.whenComplete((result, exception) -> {
			if (exception == null) {
				// Get offset from RecordMetadata

				RecordMetadata metadata = result.getRecordMetadata();

				long offset = result.getRecordMetadata().offset();
				int partition = result.getRecordMetadata().partition();

				logger.info("metadata" + metadata);
			
				//System.out.println("metadata" + metadata);
				//System.out.println("Message sent to topic " + metadata.topic() + " at offset " + metadata.offset() + " TimeStamp " + metadata.timestamp());

				System.out.println("Message sent successfully! Offset: " + offset + ", Partition: " + partition);
			} else {
				System.err.println("Failed to send message: " + exception.getMessage());
			}
		});

		System.out.println("Message sent : " + jsonPayload);
	}
}