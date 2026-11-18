package com.shubhoom.orders.notification;

import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/notifications")
class NotificationController {

    private final JdbcClient jdbc;

    NotificationController(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/{orderId}")
    List<String> forOrder(@PathVariable UUID orderId) {
        return jdbc.sql("SELECT type FROM notifications WHERE order_id = :id ORDER BY id")
                .param("id", orderId).query(String.class).list();
    }
}
