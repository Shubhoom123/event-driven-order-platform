package com.shubhoom.orders.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// Scan the parent package so the shared beans in :common are picked up.
@SpringBootApplication(scanBasePackages = "com.shubhoom.orders")
public class OrderServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
