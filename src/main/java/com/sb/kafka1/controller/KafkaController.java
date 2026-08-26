package com.sb.stock.trade.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.sb.stock.trade.service.KafkaProducerService;

@RestController
public class KafkaController {

	private final KafkaProducerService kafkaProducerService;

	public KafkaController(KafkaProducerService kafkaProducerService) {
		this.kafkaProducerService = kafkaProducerService;
	}

	@GetMapping("/send")
	public String sendMessage(@RequestParam String message) {
		kafkaProducerService.sendMessage(message);
		return "Message sent successfully";
	}

	@GetMapping("/send-new")
	public String sendNewMessage(@RequestParam String message) {
		kafkaProducerService.sendNewMessage(message);
		return "Message sent successfully";
	}
}