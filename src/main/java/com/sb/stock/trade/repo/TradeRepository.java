package com.sb.stock.trade.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.sb.stock.trade.model.Trade;

@Repository
public interface TradeRepository  extends JpaRepository<Trade, Integer>{
	
	//
	//

}
