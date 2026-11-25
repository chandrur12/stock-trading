package com.sb.stock.trade.kafka.service.impl;

import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// 1. Enums and Data Models
enum OrderType { BUY, SELL }
enum OrderStatus { PENDING, PARTIAL, FILLED, REJECTED, CANCELLED }

class Account {
    private String accountId;
    private double balance;

    public Account(String accountId, double balance) {
        this.accountId = accountId;
        this.balance = balance;
    }

    public synchronized boolean deductBalance(double amount) {
        if (balance >= amount) {
            balance -= amount;
            return true;
        }
        return false;
    }

    public synchronized void addBalance(double amount) {
        this.balance += amount;
    }

    public double getBalance() { return balance; }
    public String getAccountId() { return accountId; }
}

class Order implements Comparable<Order> {
    private String orderId;
    private String accountId;
    private String ticker;
    private int quantity;
    private double price;
    private OrderType type;
    private OrderStatus status;

    public Order(String orderId, String accountId, String ticker, int quantity, double price, OrderType type) {
        this.orderId = orderId;
        this.accountId = accountId;
        this.ticker = ticker;
        this.quantity = quantity;
        this.price = price;
        this.type = type;
        this.status = OrderStatus.PENDING;
    }

    // Sort BUY orders by highest price (desc), SELL orders by lowest price (asc)
    @Override
    public int compareTo(Order other) {
        if (this.type == OrderType.BUY) {
            return Double.compare(other.price, this.price); // Descending for buy
        } else {
            return Double.compare(this.price, other.price); // Ascending for sell
        }
    }

    // Getters
    public String getOrderId() { return orderId; }
    public String getAccountId() { return accountId; }
    public String getTicker() { return ticker; }
    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }
    public double getPrice() { return price; }
    public OrderType getType() { return type; }
    public OrderStatus getStatus() { return status; }
    public void setStatus(OrderStatus status) { this.status = status; }
}

// 2. The Order Book for a specific stock
class OrderBook {
    private final String ticker;
    private final PriorityQueue<Order> buyOrders;
    private final PriorityQueue<Order> sellOrders;

    public OrderBook(String ticker) {
        this.ticker = ticker;
        this.buyOrders = new PriorityQueue<>();
        this.sellOrders = new PriorityQueue<>();
    }

    public synchronized void addOrder(Order order) {
        if (order.getType() == OrderType.BUY) {
            buyOrders.add(order);
        } else {
            sellOrders.add(order);
        }
        matchOrders();
    }

    private void matchOrders() {
        while (!buyOrders.isEmpty() && !sellOrders.isEmpty()) {
            Order bestBuy = buyOrders.peek();
            Order bestSell = sellOrders.peek();

            // Matching logic: Buy price >= Sell price
            if (bestBuy.getPrice() >= bestSell.getPrice()) {
                int tradeQuantity = Math.min(bestBuy.getQuantity(), bestSell.getQuantity());
                double tradePrice = bestSell.getPrice(); // Maker's price dominates
                
                System.out.println("Execution: Matched " + tradeQuantity + " shares of " + ticker + 
                                   " at $" + tradePrice + ". BUY ID: " + bestBuy.getOrderId() + 
                                   ", SELL ID: " + bestSell.getOrderId());

                // Update Buy Order
                if (bestBuy.getQuantity() == tradeQuantity) {
                    bestBuy.setStatus(OrderStatus.FILLED);
                    buyOrders.poll();
                } else {
                    bestBuy.setQuantity(bestBuy.getQuantity() - tradeQuantity);
                    bestBuy.setStatus(OrderStatus.PARTIAL);
                }

                // Update Sell Order
                if (bestSell.getQuantity() == tradeQuantity) {
                    bestSell.setStatus(OrderStatus.FILLED);
                    sellOrders.poll();
                } else {
              //      bestSell.getQuantity() - tradeQuantity;
                    bestSell.setStatus(OrderStatus.PARTIAL);
                }
            } else {
                break; // No more matches
            }
        }
    }
}

// 3. Main Trading Engine
public class TradingEngine {
    private final ConcurrentHashMap<String, OrderBook> orderBooks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Account> accounts = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    public void registerAccount(Account account) {
        accounts.put(account.getAccountId(), account);
    }

    public void submitOrder(Order order) {
        executor.submit(() -> {
            Account account = accounts.get(order.getAccountId());
            if (account == null) {
                System.out.println("Rejected " + order.getOrderId() + ": Account not found.");
                return;
            }

            // Validate BUY side balance (requires strict capital validation)
            if (order.getType() == OrderType.BUY) {
                double totalCost = order.getQuantity() * order.getPrice();
                if (!account.deductBalance(totalCost)) {
                    System.out.println("Rejected " + order.getOrderId() + ": Insufficient funds.");
                    order.setStatus(OrderStatus.REJECTED);
                    return;
                }
            }

            // Process order in the specific stock's order book
            orderBooks.putIfAbsent(order.getTicker(), new OrderBook(order.getTicker()));
            OrderBook book = orderBooks.get(order.getTicker());
            book.addOrder(order);
        });
    }

    public void shutdown() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    // 4. Test the Engine
    public static void main(String[] args) throws InterruptedException {
        TradingEngine engine = new TradingEngine();

        // Setup Accounts
        Account acc1 = new Account("ACC001", 15000.0);
        Account acc2 = new Account("ACC002", 10000.0);
        engine.registerAccount(acc1);
        engine.registerAccount(acc2);

        // Submit Orders
        System.out.println("Submitting orders...");
        engine.submitOrder(new Order("ORD001", "ACC001", "AAPL", 100, 150.0, OrderType.BUY));
        engine.submitOrder(new Order("ORD002", "ACC002", "AAPL", 80, 149.0, OrderType.SELL));
        engine.submitOrder(new Order("ORD003", "ACC002", "AAPL", 20, 148.0, OrderType.SELL));

        Thread.sleep(2000); // Wait for async execution to complete
        engine.shutdown();
    }
}
