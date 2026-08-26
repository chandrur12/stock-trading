package com.sb.stock.trade.kafka.service.impl;

import org.springframework.beans.factory.annotation.Autowired;

import com.sb.stock.trade.kafka.service.KafkaProducerService;
import com.sb.stock.trade.kafka.service.PlaceOrderKafkaProducerService;
import com.sb.stock.trade.view.PlaceOrderVo;

public class PlaceOrderKafkaProducerServiceImpl  implements PlaceOrderKafkaProducerService{

	
	@Autowired 
	KafkaProducerService KafkaProducerService;
	
	@Override
	public void sendOrder(PlaceOrderVo placeOrderVo) {
		
	
		
	}

}
