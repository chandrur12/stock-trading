package com.sb.stock.trade.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import com.sb.stock.avro.StockOrder;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

@Configuration
public class KafkaAvroConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${spring.kafka.properties.schema.registry.url:http://localhost:8081}")
	private String schemaRegistryUrl;

	@Bean
	public ProducerFactory<String, StockOrder> avroProducerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

		// Enables idempotence & acks=all automatically
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

		DefaultKafkaProducerFactory<String, StockOrder> factory = new DefaultKafkaProducerFactory<>(props);
		factory.setTransactionIdPrefix("tx-order-");
		return factory;
	}

	@Bean
	public KafkaTransactionManager<String, StockOrder> kafkaTransactionManager(
			ProducerFactory<String, StockOrder> avroProducerFactory) {
		return new KafkaTransactionManager<>(avroProducerFactory);
	}

	@Bean
	public KafkaTemplate<String, StockOrder> avroKafkaTemplate(
			ProducerFactory<String, StockOrder> avroProducerFactory) {
		return new KafkaTemplate<>(avroProducerFactory);
	}

	@Bean
	public ConsumerFactory<String, StockOrder> avroConsumerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// CRITICAL for topicPattern: discover new runtime topics within 2 seconds
		props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 2000);

		// Mandatory for EOS: let KafkaTransactionManager commit offsets and ignore
		// aborted messages
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

		props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

		// Configure KafkaAvroDeserializer directly so specific.avro.reader=true cannot
		// be ignored
		KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer();
		avroDeserializer.configure(props, false);

		@SuppressWarnings("unchecked")
		Deserializer<StockOrder> valueDeserializer = new ErrorHandlingDeserializer<>(
				(Deserializer<StockOrder>) (Deserializer<?>) avroDeserializer);

		return new DefaultKafkaConsumerFactory<>(props, new ErrorHandlingDeserializer<>(new StringDeserializer()),
				valueDeserializer);
	}

	
	@Bean 	// Rollback Processor for Failed Avro Transactions -> DLT
	public DefaultAfterRollbackProcessor<String, StockOrder> afterRollbackProcessor(
			KafkaTemplate<String, StockOrder> avroKafkaTemplate) {

		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(avroKafkaTemplate,
				(record, ex) -> new TopicPartition(record.topic() + ".DLT", 0));
	
		DefaultAfterRollbackProcessor<String, StockOrder> processor = new DefaultAfterRollbackProcessor<>(recoverer,
				new FixedBackOff(1000L, 2L), avroKafkaTemplate, true // commitRecovered = true
		);

		// Skip retries for permanent validation errors and send straight to DLT
		processor.addNotRetryableExceptions(IllegalArgumentException.class);

		return processor;
	}

	@Bean("avroKafkaListenerContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, StockOrder> avroKafkaListenerContainerFactory(
			ConsumerFactory<String, StockOrder> avroConsumerFactory,
			KafkaTransactionManager<String, StockOrder> kafkaTransactionManager,
			DefaultAfterRollbackProcessor<String, StockOrder> afterRollbackProcessor) {

		ConcurrentKafkaListenerContainerFactory<String, StockOrder> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(avroConsumerFactory);
		factory.setBatchListener(false);
		factory.getContainerProperties().setKafkaAwareTransactionManager(kafkaTransactionManager);
		factory.setAfterRollbackProcessor(afterRollbackProcessor);
		return factory;
	}

	// Add inside KafkaAvroConfig.java
	@Bean
	public NewTopic tickerRelianceDltTopic() {
		return TopicBuilder.name("TICKER_RELIANCE.DLT").partitions(3) // Must be >= partition count of TICKER_RELIANCE
				.replicas(1).build();
	}

	@Bean("dltKafkaListenerContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, StockOrder> dltKafkaListenerContainerFactory(
			ConsumerFactory<String, StockOrder> avroConsumerFactory) {

		ConcurrentKafkaListenerContainerFactory<String, StockOrder> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(avroConsumerFactory);
		// No afterRollbackProcessor attached so DLT processing never loops into
		// .DLT.DLT
		return factory;
	}

}
