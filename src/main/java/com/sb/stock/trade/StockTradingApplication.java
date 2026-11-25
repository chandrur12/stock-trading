package com.sb.stock.trade;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling // Enables Spring's scheduled task execution capability
@EnableKafka
public class StockTradingApplication {

	public static void main(String[] args) {
		SpringApplication.run(StockTradingApplication.class, args);
	}

	
}
