package com.sb.stock.trade.kafka.service;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.sb.stock.trade.service.OrderService;
import com.sb.stock.trade.service.impl.StockServiceImpl;
import com.sb.stock.trade.view.PlaceOrderVo;

@Service
public class KafkaConsumerService {

	private static final Logger logger = LoggerFactory.getLogger(StockServiceImpl.class);

	@Autowired
	private OrderService orderService;

	@SuppressWarnings("unchecked")
	//@KafkaListener(topicPattern = "TickerOld.*")
	public void reciveOrderMessage(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {

		logger.info("inside reciveOrderMessage");

		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);

		List<PlaceOrderVo> placeOrderVos = new ArrayList<>();
		for (ConsumerRecord<String, String> record : records) {

			String str = record.value() + "";

			PlaceOrderVo placeOrderVo = null;
			try {
				placeOrderVo = mapper.readValue(str, PlaceOrderVo.class);
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if (placeOrderVo != null)
				placeOrderVos.add(placeOrderVo);

		}
		List<BigInteger> idlist = null;
		Map<String, Object> map = orderService.createUpdateOrder(placeOrderVos);

		if (map.containsKey("idList") && map.get("idList") != null)
			idlist = (List<BigInteger>) map.get("idList");

		if (idlist != null && idlist.size() > 0) {
			ack.acknowledge();

		}
		// Commit offset manually after reliable execution

	}

	public static void main(String[] args) throws Exception {
		ObjectMapper mapper = new ObjectMapper();

		// 2. JSON String to Java Object (Deserialization)
		String inputJson = "{\"stockId\":12,\"traderId\":21,\"ticker\":\"AAPL19\",\"orderSide\":\"BUY\",\"orderType\":\"SELL\",\"price\":100.0,\"quantity\":123}";
		// PlaceOrderVo placeOrderVo = mapper.readValue(inputJson, PlaceOrderVo.class);

		Gson gson = new Gson();

		PlaceOrderVo placeOrderVo = gson.fromJson(inputJson, PlaceOrderVo.class);
		// System.out.println("Java Object Name: " + placeOrderVo.getTicker());

		JsonParser jsonParser = new JsonParser();
		JsonElement st = jsonParser.parse(inputJson);

		Gson googleJson = new Gson();
		PlaceOrderVo p = googleJson.fromJson(st, PlaceOrderVo.class);

		System.out.println("Java Object Name: " + p.getTicker());

		// example 3 - parsing JSON using JSON-Simple in Java
		JSONParser parser = new JSONParser();
		try {
			JSONObject json = (JSONObject) parser.parse(inputJson);
			BigInteger stockId = new BigInteger(json.getOrDefault("stockId", 1).toString());
			BigInteger BigInteger = new BigInteger(json.getOrDefault("BigInteger", 1).toString());

			String ticker = (String) json.getOrDefault("ticker", "");

			String orderSide = (String) json.getOrDefault("orderSide", "");
			String orderType = (String) json.getOrDefault("orderType", "");

			BigDecimal price = new BigDecimal(json.getOrDefault("price", 0.0).toString());
			BigInteger quantity = new BigInteger(json.getOrDefault("author", 10).toString());

			System.out.println(
					new PlaceOrderVo(stockId, BigInteger, ticker, orderSide, orderType, price, quantity).toString());

		} catch (ParseException e) {
			e.printStackTrace();
		}

		System.out.println("Java object after parsing JSON using json-simple");

	}

}
