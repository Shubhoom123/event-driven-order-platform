package com.shubhoom.orders.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.util.List;

public record CreateOrderRequest(
        @NotBlank String customerId,
        @NotEmpty List<@Valid Item> items,
        @Positive long amountCents,
        @Pattern(regexp = "[A-Z]{3}") String currency) {

    public record Item(@NotBlank String sku, @Min(1) int quantity) {}
}
