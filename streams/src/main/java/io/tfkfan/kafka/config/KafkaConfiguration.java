package io.tfkfan.kafka.config;

import io.tfkfan.generated.Payment;
import io.tfkfan.kafka.Topics;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfiguration {
    @Bean
    public KStream<String, Payment> transactionStream(StreamsBuilder streamsBuilder) {
        return streamsBuilder.stream(Topics.PAYMENTS);
    }
}
