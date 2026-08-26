package com.sb.stock.trade.model;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;

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
@Table(name = "order")
public class Order {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private BigInteger id;

	@Column(name = "stock_id")
	private BigInteger stockId;
	
	@Column(name = "trader_id")
	public String traderId;
	
	@Column(name = "ticker")
	public String ticker;
	
	@Column(name = "order_side")
	public String orderSide;
	
	@Column(name = "order_type")
	public String orderType;
	
	@Column(name = "price")
	public BigDecimal price;
	
	@Column(name = "original_quantity")	// null for market orders
	public int originalQuantity;
	
	@Column(name = "remaining_quantity")
	private int remainingQuantity;

	@Column(name = "timestamp")
	public Instant timestamp;
	
	@Column(name = "status")
	public String status;

	public int getRemainingQuantity() {
		return remainingQuantity;
	}

	public void reduceRemaining(int q) {
		if (q < 0)
			throw new IllegalArgumentException("q < 0");
		if (q > remainingQuantity)
			throw new IllegalArgumentException("reduce > remaining");
		remainingQuantity -= q;
	}

	public boolean isFilled() {
		return remainingQuantity == 0;
	}

	@Override
	public String toString() {
		return String.format("Order{id=%d, trader=%s, %s %s %s x%d rem=%d}", id, traderId, orderSide, orderType, ticker,
				originalQuantity, remainingQuantity);
	}

}
