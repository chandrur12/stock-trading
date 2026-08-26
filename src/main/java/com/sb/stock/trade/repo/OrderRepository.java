package com.sb.stock.trade.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import com.sb.stock.trade.model.Order;

public interface OrderRepository  extends JpaRepository<Order, Integer>{
	
	//
	//

}
