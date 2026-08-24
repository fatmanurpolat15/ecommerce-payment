package com.fatmanur.payment.consumer;

import com.fatmanur.payment.dto.PaymentRequest;
import com.fatmanur.payment.dto.PaymentResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentRequestConsumer {

    private final KafkaTemplate<String, PaymentResult> kafkaTemplate;

    @KafkaListener(topics = "payment.requests", groupId = "payment-service")
    public void handlePaymentRequest(PaymentRequest request) {
        log.info("Payment request received for order: {}", request.orderNumber());

        try {
            Thread.sleep(2000 + (long) (Math.random() * 3000));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        boolean success = Math.random() < 0.7;
        String reference = "PAY-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        PaymentResult result = new PaymentResult(
                request.orderId(),
                success ? "SUCCESS" : "FAILED",
                reference,
                success ? null : "Insufficient funds",
                request.amount(),
                LocalDateTime.now()
        );

        kafkaTemplate.send("payment.results", request.orderId().toString(), result);
        log.info("Payment result sent for order {}: {}", request.orderNumber(), result.status());
    }
}
