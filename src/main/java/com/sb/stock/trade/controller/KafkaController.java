package com.sb.stock.trade.controller;

import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.sb.stock.trade.kafka.service.KafkaProducerService;
import com.sb.stock.trade.view.PlaceOrderVo;

@RestController
public class KafkaController {

	private final StockController stockController;
	private final KafkaProducerService kafkaProducerService;

	public KafkaController(KafkaProducerService kafkaProducerService, StockController stockController) {
		this.kafkaProducerService = kafkaProducerService;
		this.stockController = stockController;
	}

	@GetMapping("/send")
	public String sendMessage(@RequestParam String message) {
		kafkaProducerService.sendMessage(message);
		return "Message sent successfully";
	}
	@PostMapping("/send-by-topic")
	public String sendMessage(@RequestBody Map<String, Object> map) {
		

		kafkaProducerService.sendMessage(map.get("topic").toString(), map.get("msg").toString());


		return "Message sent successfully";
	}

	@PostMapping("/send-new")
	public String sendNewMessage(@RequestBody PlaceOrderVo placeOrderVo) {
	
		PlaceOrderVo placeOrderVoLocal= new PlaceOrderVo();
		
		placeOrderVoLocal.setStockId(placeOrderVo.getStockId());
		placeOrderVoLocal.setTraderId(placeOrderVo.getTraderId());
		placeOrderVoLocal.setTicker(placeOrderVo.getTicker());
		placeOrderVoLocal.setOrderSide(placeOrderVo.getOrderSide());

		placeOrderVoLocal.setOrderType(placeOrderVo.getOrderType());
		
		placeOrderVoLocal.setPrice(placeOrderVo.getPrice());
		placeOrderVoLocal.setQuantity(placeOrderVo.getQuantity());

		
		String jsonPayload = null;
		try {
			jsonPayload = new ObjectMapper().writeValueAsString(placeOrderVo);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		System.out.println(jsonPayload);

		JsonParser jsonParser = new JsonParser();
    	JsonElement st = jsonParser.parse(jsonPayload);
    	
		Gson googleJson = new Gson();
		PlaceOrderVo p = googleJson.fromJson(st, PlaceOrderVo.class);
		
		System.out.println("  extract "+ p.getTicker());

		
		kafkaProducerService.sendOrderMessage(placeOrderVoLocal);
		return "Message sent successfully";
	}
}