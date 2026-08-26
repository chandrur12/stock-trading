package com.sb.stock.trade.view;

import java.math.BigDecimal;
import java.math.BigInteger;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockVo {

	private BigInteger id;
    private String symbol;
	private String companyName;
	private BigDecimal currentPrice;
	private BigDecimal previousClose;
	private BigDecimal marketCap;
	private BigInteger shares;
    
}
