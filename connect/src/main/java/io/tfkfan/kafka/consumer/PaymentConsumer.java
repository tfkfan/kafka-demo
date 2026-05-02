package io.tfkfan.kafka.consumer;

import io.tfkfan.kafka.Topics;
import io.tfkfan.kafka.repository.PaymentRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class PaymentConsumer {
    private final PaymentRepository paymentRepository;

    public PaymentConsumer(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    @KafkaListener(topics = Topics.SUMMARY_LAST_WEEK, groupId = "payments_summary_fetcher")
    public void consume(@Payload Double value, @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        paymentRepository.saveWeeklyReport(key, value);
    }
}
