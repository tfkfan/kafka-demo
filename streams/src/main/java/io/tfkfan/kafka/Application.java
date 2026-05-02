package io.tfkfan.kafka;

import io.tfkfan.generated.Payment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import io.tfkfan.kafka.streams.MessageProducer;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@SpringBootApplication
public class Application implements CommandLineRunner {
    private final MessageProducer messageProducer;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) {
        log.info("App started");
        var senderId = UUID.fromString("5f2df706-00d1-4375-814d-5771f99b4ca4");
        Payment p1 = Payment.newBuilder()
                .setFrom(senderId)
                .setTo(UUID.randomUUID())
                .setTransactionId(UUID.randomUUID())
                .setAmount(990.00)
                .setCreatedAt(System.currentTimeMillis())
                .build();

        Payment p2 = Payment.newBuilder()
                .setFrom(senderId)
                .setTo(UUID.randomUUID())
                .setTransactionId(UUID.randomUUID())
                .setAmount(10.00)
                .setCreatedAt(System.currentTimeMillis())
                .build();


        messageProducer.send("payments_topic", p1.getFrom().toString(), p1);
        messageProducer.send("payments_topic", p2.getFrom().toString(), p2);
    }
}
