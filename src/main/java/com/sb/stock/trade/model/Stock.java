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
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
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
	private BigDecimal currentPrice;
   
    @Column(name = "previous_close")
	private BigDecimal previousClose;
  
    @Column(name = "market_cap")
	public BigDecimal marketCap;
    
    @Column(name = "shares")
	public BigInteger shares;


	

}
