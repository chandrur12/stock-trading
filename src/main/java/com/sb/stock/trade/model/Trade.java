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
@Table(name = "trade")

public class Trade {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
	private BigInteger id;
 
    @Column(name = "buy_order_id")
    public  BigInteger buyOrderId;
  
    @Column(name = "sell_order_id")
    public  BigInteger sellOrderId;
    
    @Column(name = "ticker")
    public  String ticker;

    @Column(name = "price")
    public  BigDecimal price;
  
    @Column(name = "quantity")
    public  int quantity;
    
    @Column(name = "timestamp")
    public  Date timestamp;


    @Override
    public String toString() {
        return String.format("Trade{id=%d, %s %d @ %s (%s<->%s)}", id, ticker, quantity, price.toPlainString(), buyOrderId, sellOrderId);
    }
}