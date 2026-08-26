package com.sb.stock.trade.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.stereotype.Service;

@Service
public class DynamicKafkaService {

	private final KafkaAdmin kafkaAdmin;
	private final ConsumerFactory<String, String> consumerFactory;

	// Keeps track of active containers to manage lifecycle (stop/restart)
	private final Map<String, KafkaMessageListenerContainer<String, String>> activeContainers = new ConcurrentHashMap<>();

	public DynamicKafkaService(KafkaAdmin kafkaAdmin, ConsumerFactory<String, String> consumerFactory) {
		this.kafkaAdmin = kafkaAdmin;
		this.consumerFactory = consumerFactory;
	}

	// Creates a new Kafka topic at runtime and subscribes a new listener to it.
	
	public void createAndSubscribe(String topicName, int partitions, short replicationFactor) {
		// Step 1: Create the Topic programmatically
		try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
			NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
			adminClient.createTopics(Collections.singletonList(newTopic)).all().get();

			System.out.println("Successfully created topic: " + topicName);
		} catch (Exception e) {
			throw new RuntimeException("Failed to create Kafka topic: " + topicName, e);
		}

		// Step 2: Create Container Properties and attach a Message Listener
		ContainerProperties containerProperties = new ContainerProperties(topicName);

		containerProperties.setMessageListener(new BatchMessageListener<String, String>() {

			@Override
			public void onMessage(List<ConsumerRecord<String, String>> data) {

				// Handle your runtime business logic here
				System.out.printf("Received message from dynamic topics");

			}
		});

		// Step 3: Instantiate and start the dynamic Kafka Message Listener Container
		KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(consumerFactory,
				containerProperties);

		container.start(); // This triggers the background poll loop at runtime
		activeContainers.put(topicName, container);
		System.out.println("Successfully subscribed to topic: " + topicName);
	}

	//Optional: Gracefully stop a runtime consumer container when no longer needed

	public void stopSubscription(String topicName) {
		KafkaMessageListenerContainer<String, String> container = activeContainers.remove(topicName);
		if (container != null && container.isRunning()) {
			container.stop();
			System.out.println("Stopped listener for topic: " + topicName);
		}
	}
}
