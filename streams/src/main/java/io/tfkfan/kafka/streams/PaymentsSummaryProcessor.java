package io.tfkfan.kafka.streams;

import io.tfkfan.generated.Payment;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class PaymentsSummaryProcessor {

    @Autowired
    void buildPipeline(KStream<String, Payment> paymentsStream) {
        KTable<Windowed<String>, Double> aggregatedSums = paymentsStream
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(7)))
                .aggregate(
                        () -> 0d,
                        this::apply,
                        Materialized.with(Serdes.String(), Serdes.Double())
                );

        aggregatedSums
                .toStream()
                .map((windowedKey, sum) -> new KeyValue<>(windowedKey.key(), sum.toString()))
                .to("output-topic", Produced.valueSerde(Serdes.String()));
    }

    public Double apply(String key, Payment payment, Double aggregate) {
        return aggregate + payment.getAmount();
    }

}
