package com.sb.stock.trade.kafka.service;

import com.sb.stock.trade.view.PlaceOrderVo;

public interface PlaceOrderKafkaProducerService {
	
	void sendOrder(PlaceOrderVo placeOrderVo );
	
	

} 
