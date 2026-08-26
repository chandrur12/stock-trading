package com.sb.stock.trade.service.impl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.sb.stock.trade.config.Constants;
import com.sb.stock.trade.model.Orders;
import com.sb.stock.trade.repo.OrdersRepository;
import com.sb.stock.trade.service.OrderService;
import com.sb.stock.trade.view.PlaceOrderVo;

@Service
public class OrderServiceImpl implements OrderService {

	private static final Logger logger = LoggerFactory.getLogger(StockServiceImpl.class);

	@Autowired
	private OrdersRepository ordersRepository;

	@Override
	public Map<String, Object> createUpdateOrder(PlaceOrderVo placeOrderVo) {

		Map<String, Object> map = new HashMap<>();

		if (placeOrderVo != null) {
			if (placeOrderVo.getStockId() != null) {

				LocalDateTime current = LocalDateTime.now();

				Orders orders = new Orders(null, placeOrderVo.getStockId(), placeOrderVo.getTraderId(),
						placeOrderVo.getTicker(), placeOrderVo.getOrderSide(), placeOrderVo.getOrderType(),
						placeOrderVo.getPrice(), placeOrderVo.getQuantity(), placeOrderVo.getQuantity(), current,
						"Placed");

				ordersRepository.save(orders);

			}

			map.put("status", "success");

		} else {

			map.put("status", "error");
			logger.error(" Input Records ");

		}
		return null;
	}

	@Override
	public Map<String, Object> createUpdateOrder(List<PlaceOrderVo> placeOrderVos) {

		List<BigInteger> idlist = null;
		Map<String, Object> map = new HashMap<>();

		if (placeOrderVos != null && placeOrderVos.size() > 0) {
			Iterator<PlaceOrderVo> iterator = placeOrderVos.stream().iterator();

			List<Orders> list = new ArrayList<>();
			while (iterator.hasNext()) {

				PlaceOrderVo placeOrderVo = iterator.next();

				if (placeOrderVo != null) {
					if (placeOrderVo.getStockId() != null) {

						LocalDateTime current = LocalDateTime.now();

						Orders order = new Orders(null, placeOrderVo.getStockId(), placeOrderVo.getTraderId(),
								placeOrderVo.getTicker(), placeOrderVo.getOrderSide(), placeOrderVo.getOrderType(),
								placeOrderVo.getPrice(), placeOrderVo.getQuantity(), placeOrderVo.getQuantity(),
								current, "Placed");

						list.add(order);
					}
				} else {
					map.put("status", "error");
					logger.error(" Input Needed ");

				}
				list = ordersRepository.saveAll(list);
				idlist = list.stream().filter(o -> o.getId() != null).map(o -> o.getId()).collect(Collectors.toList());
				map.put("status", "success");
				map.put("idList", list);
			}

		}

		return map;
	}

	@Override
	public Map<String, Object> processOrders(List<BigInteger> idlist) {

		Map<String, Object> map = new HashMap<>();

		if (idlist != null && idlist.size() > 0) {

		} else {
			
			logger.info("Processinng Orders ");

			// Map<String, Queue<Orders>> orderBook = new ConcurrentHashMap<>();
			List<Orders> ordersList = ordersRepository.findOrdersByStatus("Completed");

			
			logger.info("Processinng Orders :" + ordersList.size());

			// 2. Partition into Buy/Sell queues while preserving insertion order
			Map<BigInteger, Map<Boolean, Queue<Orders>>> stockIdPartitionedMap = ordersList.stream()
					.collect(Collectors.groupingBy(Orders::getStockId, // First level key mapper
							Collectors.partitioningBy(order -> order.getOrderType().equals("BUY"), 
									Collectors.toCollection(ConcurrentLinkedQueue::new) 
							)));

			for (Entry<BigInteger, Map<Boolean, Queue<Orders>>> entry : stockIdPartitionedMap.entrySet()) {

				System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());

				ConcurrentLinkedQueue<Orders> buyOrdersQueue = (ConcurrentLinkedQueue<Orders>) entry.getValue()
						.get(true);
				ConcurrentLinkedQueue<Orders> sellOrderQueue = (ConcurrentLinkedQueue<Orders>) entry.getValue()
						.get(false);
				List<Orders> btachOrders = new ArrayList<>();
				while (!buyOrdersQueue.isEmpty() && !sellOrderQueue.isEmpty()) {
					Orders bestBuy = buyOrdersQueue.peek();
					Orders bestSell = sellOrderQueue.peek();

					// Matching logic: Buy price >= Sell price

					if (bestBuy.getPrice().compareTo(bestSell.getPrice()) >= 0) {

						BigInteger tradeQuantity = bestBuy.getRemainingQuantity().min(bestSell.getRemainingQuantity());

						BigDecimal tradePrice = bestSell.getPrice(); // Maker's price dominates

						System.out.println("Execution: Matched " + tradeQuantity + " shares of  Stock Id "
								+ entry.getKey() + "" + " at $" + tradePrice + ". BUY ID: " + bestBuy.getId()
								+ ", SELL ID: " + bestSell.getId());

						// Update Buy Order
						if (bestBuy.getRemainingQuantity().equals(tradeQuantity)) {
							bestBuy.setStatus(Constants.CONSTANT_ORDER_STATUS_COMPLETED);
							bestBuy.setRemainingQuantity(new BigInteger("0"));

							buyOrdersQueue.poll();
						} else {

							bestBuy.setRemainingQuantity(bestBuy.getRemainingQuantity().subtract(tradeQuantity));
							bestBuy.setStatus(Constants.CONSTANT_ORDER_STATUS_PARTIAL);
						}

						// Update Sell Order
						if (bestSell.getRemainingQuantity().equals(tradeQuantity) ) {
							bestSell.setStatus(Constants.CONSTANT_ORDER_STATUS_COMPLETED);
							bestSell.setRemainingQuantity(new BigInteger("0"));

							sellOrderQueue.poll();
						} else {
							// bestSell.getQuantity() - tradeQuantity;
							bestSell.setRemainingQuantity(bestSell.getRemainingQuantity().subtract(tradeQuantity));
							bestSell.setStatus(Constants.CONSTANT_ORDER_STATUS_PARTIAL);
						}

						btachOrders.add(bestSell);
						btachOrders.add(bestBuy);

					} else {
						break; // No more matches
					}
				}
				
			logger.info("Order size "+ btachOrders.size());

				List<Orders> idObjects = ordersRepository.saveAll(btachOrders);
				List<BigInteger> ids = idObjects.stream().filter(o -> o.getId() != null).map(o -> o.getId())
						.collect(Collectors.toList());

				logger.info(" Processed  Stock Id : " + entry.getKey() + " and Order Ids : " + ids);
				map.put(entry.getKey().toString(), ids);
			}

		}

		return map;
	}

}
