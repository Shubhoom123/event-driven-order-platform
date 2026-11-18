package com.shubhoom.orders.inventory;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import java.util.Optional;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/stock")
class StockController {

    public record SetStock(@Min(0) int available) {}

    public record StockView(String sku, int available, int reserved, int sold) {}

    private final JdbcClient jdbc;

    StockController(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    // Seeding only: overwrites "available" without regard to current reservations.
    @PutMapping("/{sku}")
    StockView put(@PathVariable String sku, @Valid @RequestBody SetStock body) {
        jdbc.sql("INSERT INTO stock (sku, available) VALUES (:sku, :a) "
                        + "ON CONFLICT (sku) DO UPDATE SET available = :a")
                .param("sku", sku).param("a", body.available()).update();
        return find(sku).orElseThrow();
    }

    @GetMapping("/{sku}")
    ResponseEntity<StockView> get(@PathVariable String sku) {
        return find(sku).map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    private Optional<StockView> find(String sku) {
        return jdbc.sql("SELECT sku, available, reserved, sold FROM stock WHERE sku = :sku")
                .param("sku", sku)
                .query((rs, i) -> new StockView(rs.getString(1), rs.getInt(2), rs.getInt(3), rs.getInt(4)))
                .optional();
    }
}
