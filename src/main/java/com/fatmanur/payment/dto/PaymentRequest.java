package com.fatmanur.payment.dto;

import java.math.BigDecimal;

public record PaymentRequest(
    Long orderId,
    String orderNumber,
    BigDecimal amount,
    String customerEmail
) {}
