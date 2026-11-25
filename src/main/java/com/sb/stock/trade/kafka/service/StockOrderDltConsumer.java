package com.sb.stock.trade.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class StockOrderDltConsumer {

	private static final Logger logger = LoggerFactory.getLogger(StockOrderDltConsumer.class);


	@KafkaListener(topicPattern = ".*\\.DLT", groupId = "stock-order-dlt-group", containerFactory = "dltKafkaListenerContainerFactory")
	public void handleDeadLetter(ConsumerRecord<String, Object> record,
			@Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String errorMessage,
			@Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
			@Header(name = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false) Long originalOffset) {

		logger.info("Received DLT record: {}", record);
		logger.error("DLT Alert | Source Topic: {} | Offset: {} | Error: {} | Order: {}", originalTopic, originalOffset,
				errorMessage, record.value());
	}

}
