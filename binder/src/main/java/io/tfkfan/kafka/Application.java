package io.tfkfan.kafka;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.tfkfan.generated.Payment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@SpringBootApplication
public class Application {
    static final UUID senderId = UUID.fromString("5f2df706-00d1-4375-814d-5771f99b4ca4");

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public Serde<Payment> avroInSerde() {
        return new SpecificAvroSerde<>();
    }

    @Bean
    public Supplier<Message<Payment>> producePayments() {
        return () -> MessageBuilder.withPayload(Payment.newBuilder()
                        .setFrom(senderId)
                        .setTo(UUID.randomUUID())
                        .setTransactionId(UUID.randomUUID())
                        .setAmount(900.00)
                        .setCreatedAt(System.currentTimeMillis())
                        .build())
                .setHeader(KafkaHeaders.KEY, senderId.toString())
                .build();
    }

    @Bean
    public Function<KStream<String, Payment>, KStream<String, Double>> processPayments() {
        return paymentsStream -> paymentsStream
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(7)))
                .aggregate(
                        () -> 0d,
                        this::apply,
                        Materialized.with(Serdes.String(), Serdes.Double())
                )
                .toStream()
                .map((windowedKey, sum) -> new KeyValue<>(windowedKey.key(), sum));
    }

    private Double apply(String key, Payment payment, Double aggregate) {
        return aggregate + payment.getAmount();
    }
}
