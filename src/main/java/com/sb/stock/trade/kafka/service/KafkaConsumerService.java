package com.sb.stock.trade.kafka.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    @KafkaListener(topics = "my_topic_2", groupId = "group_id_2")
    public void consume(String message) {
        System.out.println("Message received: " + message);
    }
}

/*

void whenSendingMessagesOnTwoTopics_thenConsumerReceivesMessages() throws Exception {
    publishMessages();
    
    
    KafkaConsumerConfig.
    kafkaConsumer.subscribe(Arrays.asList("card-payments", "bank-transfers"));

    int eventsProcessed = 0;
    for (ConsumerRecord<String, String> record : kafkaConsumer.poll(Duration.ofSeconds(10))) {
        log.info("Event on topic={}, payload={}", record.topic(), record.value());
        eventsProcessed++;
    }
    assertThat(eventsProcessed).isEqualTo(2);
}
*/