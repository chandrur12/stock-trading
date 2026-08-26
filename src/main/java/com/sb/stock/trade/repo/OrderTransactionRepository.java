package com.sb.stock.trade.repo;

import java.math.BigInteger;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.sb.stock.trade.model.OrderTransaction;

@Repository
public interface OrderTransactionRepository  extends JpaRepository< OrderTransaction, BigInteger>{
	

	
	

}
