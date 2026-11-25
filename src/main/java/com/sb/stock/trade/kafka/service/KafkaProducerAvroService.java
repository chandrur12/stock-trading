package com.sb.stock.trade.kafka.service;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.sb.stock.avro.StockOrder;
import com.sb.stock.trade.view.PlaceOrderVo;

@Service
public class KafkaProducerAvroService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerAvroService.class);

    private final KafkaTemplate<String, StockOrder> avroKafkaTemplate;
    private final KafkaAdmin kafkaAdmin;

    public KafkaProducerAvroService(KafkaTemplate<String, StockOrder> avroKafkaTemplate, KafkaAdmin kafkaAdmin) {
        this.avroKafkaTemplate = avroKafkaTemplate;
        this.kafkaAdmin = kafkaAdmin;
    }

    @Transactional("kafkaTransactionManager")
    
    public void sendOrder(PlaceOrderVo orderVo) {
        String topicName = orderVo.getTicker().toUpperCase(); // e.g., TICKER_RELIANCE, TICKER_TCS

        // 1. Dynamically create the topic at runtime if it doesn't exist yet
        NewTopic topic = TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(1)
                .build();
        kafkaAdmin.createOrModifyTopics(topic);

        // 2. Build the Avro SpecificRecord
        StockOrder avroOrder = StockOrder.newBuilder()
                .setStockId(new BigDecimal(orderVo.getStockId()))
                .setTraderId(new BigDecimal(orderVo.getTraderId()))
                .setTicker(topicName)
                .setOrderSide(orderVo.getOrderSide().toUpperCase())
                .setOrderType(orderVo.getOrderType().toUpperCase())
                .setPrice(orderVo.getPrice().setScale(4, RoundingMode.HALF_UP))
                .setQuantity(new BigDecimal(orderVo.getQuantity()))
                .build();

        // 3. Publish to the dynamic topic (using traderId or ticker as partition key)
        avroKafkaTemplate.send(topicName, topicName, avroOrder).whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info("Published {} to topic [{}], partition [{}], offset [{}]",
                        orderVo,
                        topicName,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                logger.error("Failed to publish {} to topic [{}]", orderVo, topicName, ex);
            }
        });
    }
}
