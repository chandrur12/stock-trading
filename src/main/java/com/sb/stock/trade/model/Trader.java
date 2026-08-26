package com.sb.stock.trade.model;

import java.math.BigInteger;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "trader")
public class Trader {
	

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
	private BigInteger id;
  
    @Column(name = "name")
    public  String name;
    
    @Column(name = "adress")
    public  String adress;
    
    @Column(name = "pan")
    public  String pan;

}
