package com.sb.stock.trade.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import com.sb.stock.trade.model.PriceHistory;

public interface PriceHistoryRepository  extends JpaRepository<PriceHistory, Integer>{
	
	//
	//

}
