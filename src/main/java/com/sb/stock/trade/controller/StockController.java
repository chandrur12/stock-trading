package com.sb.stock.trade.controller;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sb.stock.trade.service.StockService;
import com.sb.stock.trade.view.StockVo;

@RestController
@RequestMapping("/stock")
public class StockController {

	@Autowired
	private StockService stockService;

	@PostMapping("/create-update")
	public ResponseEntity save(@RequestBody StockVo stockVo) {

		Map<String, Object> map = stockService.createUpdateStock(stockVo);

		if (map.containsKey("status") && map.get("status") == "success")
			return ResponseEntity.ok(map);
		else
			return ResponseEntity.badRequest().body(map);

	}

	@GetMapping("/findall")
	public ResponseEntity<List<StockVo>> findAll() {
		Map<String, Object> map = stockService.createUpdateStock(null);

		return ResponseEntity.ok(null);
	}

}