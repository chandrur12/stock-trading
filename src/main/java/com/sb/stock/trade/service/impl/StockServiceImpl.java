package com.sb.stock.trade.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.sb.stock.trade.config.DynamicKafkaService;
import com.sb.stock.trade.model.Stock;
import com.sb.stock.trade.repo.StockRepository;
import com.sb.stock.trade.service.StockService;
import com.sb.stock.trade.view.StockVo;

@Service
public class StockServiceImpl implements StockService {

	private static final Logger logger = LoggerFactory.getLogger(StockServiceImpl.class);

	@Autowired
	private  DynamicKafkaService dynamicKafkaService;
	
	@Autowired
	private StockRepository stockRepository;

	@Override
	public Map<String, Object> createUpdateStock(StockVo stockVo) {

		Map<String, Object> map = new HashMap<>();

		if (stockVo != null) {

			if (stockVo.getId() != null) {

				Optional<Stock> stocks = stockRepository.findById(stockVo.getId());
				Stock stock = stocks.get();
				stock.setCurrentPrice(stockVo.getCurrentPrice());
				stock.setPreviousClose(stockVo.getPreviousClose());
				stock.setMarketCap(stockVo.getMarketCap());
				stock.setShares(stockVo.getShares());
				stockRepository.save(stock);
				

			} else {
				Stock stock = new Stock(null, stockVo.getSymbol(), stockVo.getCompanyName(), stockVo.getCurrentPrice(),
						stockVo.getPreviousClose(), stockVo.getMarketCap(), stockVo.getShares());
				stockRepository.save(stock);
				
				short a= 1;
				
				dynamicKafkaService.createAndSubscribe(stockVo.getSymbol(), 1, a);
		//		dynamicKafkaListenerService.createAndStartListener(stockVo.getCompanyName(), "Trade");

				map.put("status", "success");
				logger.error(" Stock Record Created  ");

			}

		} else {
			map.put("status", "error");

			logger.error(" Input Needed ");
		}

		return map;
	}

}