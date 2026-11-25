package com.sb.stock.trade.view;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PlaceOrderVo implements Serializable {

    private static final long serialVersionUID = 1L;

    private BigInteger stockId;
    private BigInteger traderId;
    private String ticker;
    private String orderSide;
    private String orderType;
    private BigDecimal price;
    private BigInteger quantity;
}
