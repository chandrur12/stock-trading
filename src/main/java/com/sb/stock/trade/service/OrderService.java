package com.sb.stock.trade.service;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import com.sb.stock.trade.view.PlaceOrderVo;

public interface OrderService {
	
	Map<String, Object> createUpdateOrder(PlaceOrderVo placeOrderVo);

	Map<String, Object> createUpdateOrder(List<PlaceOrderVo> placeOrderVos);
	
	Map<String, Object> processOrders(	List<BigInteger> idlist);

}
