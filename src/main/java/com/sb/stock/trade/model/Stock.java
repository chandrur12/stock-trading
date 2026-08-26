package com.sb.stock.trade.model;

import java.math.BigDecimal;
import java.math.BigInteger;

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
@Table(name = "stock")
public class Stock {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
	private BigInteger id;
	
    @Column(name = "symbol")
    private String symbol;

    @Column(name = "company_name")
	private String companyName;

    @Column(name = "current_price")
	private double currentPrice;
   
    @Column(name = "previous_close")
	private double previousClose;
  
    @Column(name = "market_cap")
	public BigDecimal marketCap;
    
    @Column(name = "shares")
	public BigInteger shares;

	// Constructor
	public Stock(String symbol, String companyName, double initialPrice) {
		this.symbol = symbol;
		this.companyName = companyName;
		this.currentPrice = initialPrice;
		this.previousClose = initialPrice; // Baseline for calculating daily change
	}

	
	// Methods
	public void updatePrice(double newPrice) {
		this.previousClose = this.currentPrice;
		this.currentPrice = newPrice;
	}

	public double calculateDailyChange() {
		return currentPrice - previousClose;
	}

	public double calculatePercentageChange() {
		if (previousClose == 0)
			return 0.0;
		return ((currentPrice - previousClose) / previousClose) * 100;
	}
}
