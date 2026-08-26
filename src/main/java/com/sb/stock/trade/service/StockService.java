package com.sb.stock.trade.service;

import java.util.Map;

import com.sb.stock.trade.view.StockVo;

public interface StockService {
	
	Map<String, Object> createUpdateStock(StockVo stockVo);

}
