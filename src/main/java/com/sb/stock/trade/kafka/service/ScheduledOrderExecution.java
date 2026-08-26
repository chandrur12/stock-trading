package com.sb.stock.trade.kafka.service;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.annotation.Schedules;
import org.springframework.stereotype.Component;

import com.sb.stock.trade.service.OrderService;
@Component
public class ScheduledOrderExecution {
	private static final Logger log = LoggerFactory.getLogger(ScheduledOrderExecution.class);

	@Autowired
	OrderService orderService;

	@Schedules({
			// Runs every minute from 9:00 AM to 2:59 PM (9-14)
			@Scheduled(cron = "0  0/2 9-14 * * MON-FRI"),
			// Runs exactly at 3:00 PM
			@Scheduled(cron = "0 0 15 * * MON-FRI") })
	public void runWithCronExpression() {

		log.info("Cron Task execution Starting at: {}", LocalDateTime.now());
		orderService.processOrders(null);
		log.info("Cron Task executed at: {}", LocalDateTime.now());
	}

}
