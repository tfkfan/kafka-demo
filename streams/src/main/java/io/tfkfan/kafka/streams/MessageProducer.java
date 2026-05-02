package io.tfkfan.kafka.streams;

import io.tfkfan.generated.Payment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageProducer {
    private final KafkaTemplate<String, Payment> kafkaTemplate;

    public CompletableFuture<SendResult<String, Payment>> send(String topic, String key, Payment model) {
        return kafkaTemplate.send(topic, key, model);
    }
}
