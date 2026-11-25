package com.sb.stock.trade.kafka.service;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.sb.stock.avro.StockOrder;
import com.sb.stock.trade.view.PlaceOrderVo;

@Service
public class StockOrderConsumer {

	private static final Logger logger = LoggerFactory.getLogger(StockOrderConsumer.class);
	/*
	 * @KafkaListener( topics = "TICKER_RELIANCE", groupId =
	 * "stock-trading-avro-group", containerFactory =
	 * "avroKafkaListenerContainerFactory") // @KafkaListener(topicPattern =
	 * "TICKER.*", groupId = "stock-trading-avro-group", containerFactory =
	 * "avroKafkaListenerContainerFactory") public void consumeOrder(StockOrder
	 * avroOrder, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
	 * 
	 * @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
	 * 
	 * PlaceOrderVo orderVo =
	 * PlaceOrderVo.builder().stockId(avroOrder.getStockId().toBigIntegerExact())
	 * .traderId(avroOrder.getTraderId().toBigIntegerExact()).ticker(avroOrder.
	 * getTicker())
	 * .orderSide(avroOrder.getOrderSide()).orderType(avroOrder.getOrderType()).
	 * price(avroOrder.getPrice())
	 * .quantity(avroOrder.getQuantity().toBigIntegerExact()).build();
	 * 
	 * logger.info("Consumed from topic [{}], partition [{}]: {}", topic, partition,
	 * orderVo); }
	 */

	@KafkaListener(topicPattern = "^TICKER_(?!.*\\.DLT$).*", groupId = "stock-trading-avro-group", containerFactory = "avroKafkaListenerContainerFactory")
	@Transactional("kafkaTransactionManager")

	public void consumeOrder(ConsumerRecord<String, Object> record) {
		if (record.value() == null) {
			logger.error("Payload is NULL! Headers: {}", record.headers());
			return;
		}
		logger.info("Received class: {} | Value: {}", record.value().getClass().getName(), record.value());
		PlaceOrderVo orderVo;
		if (record.value() instanceof StockOrder avroOrder) {
			logger.info("Consumed avroOrder: {}", avroOrder);

			orderVo = PlaceOrderVo.builder().stockId(avroOrder.getStockId().toBigIntegerExact())
					.traderId(avroOrder.getTraderId().toBigIntegerExact()).ticker(avroOrder.getTicker())
					.orderSide(avroOrder.getOrderSide()).orderType(avroOrder.getOrderType()).price(avroOrder.getPrice())
					.quantity(avroOrder.getQuantity().toBigIntegerExact()).build();
			logger.info("Consumed order: {}", orderVo);
		} else if ((record.value() instanceof GenericRecord generic)) {
			orderVo = PlaceOrderVo.builder().stockId(new BigInteger(generic.get("stockId").toString()))
					.traderId(new BigInteger(generic.get("traderId").toString()))
					.ticker(generic.get("ticker").toString()).orderSide(generic.get("orderSide").toString())
					.orderType(generic.get("orderType").toString())
					.price(new BigDecimal(generic.get("price").toString()))
					.quantity(new BigInteger(generic.get("quantity").toString())).build();

			BigInteger bigNum = orderVo.getQuantity();
			int smallNum = 10;

			if (bigNum.compareTo(BigInteger.valueOf(smallNum)) < 0) {
				logger.error("Invalid quantity ({}) for order {}. Triggering rollback!", orderVo.getQuantity(),
						orderVo.getStockId());
				throw new IllegalArgumentException("Order quantity must be positive");
			}

			logger.info("Consumed generic order: {}", orderVo);

		} else {
			logger.info("Unexpected payload type: {}", record.value().getClass().getName());
			return;
		}

	}

}
