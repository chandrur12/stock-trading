package com.sb.stock.trade.repo;

import java.math.BigInteger;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.sb.stock.trade.model.Orders;

@Repository
public interface OrdersRepository extends JpaRepository<Orders, BigInteger> {

	@Query(value = "  select  * FROM orders WHERE status <> :status order by id ", nativeQuery = true)
	List<Orders> findOrdersByStatus(@Param("status") String status);

}
