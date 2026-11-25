package com.sb.stock.trade.model;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "order_transaction")
public class  OrderTransaction {
	
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private BigInteger id;

	@Column(name = "order_id")
	private BigInteger orderId;
		
	@Column(name = "txn_alue")
	public BigDecimal txnValue;
	
	@Column(name = "original_quantity")	// null for market orders
	public int originalQuantity;
	
	@Column(name = "remaining_quantity")
	private int remainingQuantity;

	@Column(name = "txn_Completion_time")
	public Date txnCompletetionTime;


}
