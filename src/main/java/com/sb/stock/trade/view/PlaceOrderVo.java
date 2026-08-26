package com.sb.stock.trade.view;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;



@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@Getter
@Setter
@AllArgsConstructor

public class PlaceOrderVo  implements Serializable{


	@SuppressWarnings("unused")
	private BigInteger stockId;
	private BigInteger traderId;
	private String ticker;
	private String orderSide;
	private String orderType;
	private BigDecimal price;
	private BigInteger quantity;


	@Override
	public String toString() {
		return "PlaceOrderVo [stockId=" + stockId + ", traderId=" + traderId + ", ticker=" + ticker + ", orderSide="
				+ orderSide + ", orderType=" + orderType + ", price=" + price + ", quantity=" + quantity + "]";
	}
	
	





}
