package com.fatmanur.payment.consumer;

import tools.jackson.databind.ObjectMapper;
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

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "payment.requests", groupId = "payment-service")
    public void handlePaymentRequest(String message) {
        try {
            PaymentRequest request = objectMapper.readValue(message, PaymentRequest.class);
            log.info("Payment request received for order: {}", request.orderNumber());

            Thread.sleep(2000 + (long) (Math.random() * 3000));

            boolean success = Math.random() < 0.7;
            String reference = "PAY-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

            PaymentResult result = new PaymentResult(
                    request.orderId(),
                    success ? "SUCCEEDED" : "FAILED",
                    reference,
                    success ? null : "Insufficient funds",
                    request.amount(),
                    LocalDateTime.now()
            );

            String resultJson = objectMapper.writeValueAsString(result);
            kafkaTemplate.send("payment.results", request.orderId().toString(), resultJson);
            log.info("Payment result sent for order {}: {}", request.orderNumber(), result.status());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Failed to process payment request: {}", message, e);
        }
    }
}
