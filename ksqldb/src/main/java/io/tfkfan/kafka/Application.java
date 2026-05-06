package io.tfkfan.kafka;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;
import static java.lang.StringTemplate.STR;
@Slf4j
public class Application {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final ClientOptions options = ClientOptions.create()
                .setHost("localhost")
                .setPort(8088);

        final String inputStream = "PAYMENTS_STREAM";
        final String aggregationTable = "PAYMENTS_AGGREGATION";
        final String intermediateTableStream = "PAYMENTS_AGGREGATION_INTERMEDIATE_STREAM";
        final String finalStream = "PAYMENTS_FINAL_STREAM";

        try (final Client client = Client.create(options)) {
            client.executeStatement(STR."""
                CREATE OR REPLACE STREAM \{inputStream} (userId STRING KEY, amount DOUBLE)
                WITH (KAFKA_TOPIC='\{Topics.PAYMENTS}', VALUE_FORMAT='AVRO');
                """).get();
            client.executeStatement(STR."""
                CREATE TABLE IF NOT EXISTS \{aggregationTable} AS
                SELECT userId,
                SUM(amount) as amount,
                from_unixtime(WINDOWSTART) as window_start,
                from_unixtime(WINDOWEND) as window_end,
                from_unixtime(max(ROWTIME)) as window_emit
                FROM \{inputStream} WINDOW TUMBLING (SIZE 7 DAYS) GROUP BY userId EMIT CHANGES;
                """).get();
            client.executeStatement(STR."""
                CREATE OR REPLACE STREAM \{intermediateTableStream} (userId STRING KEY, amount DOUBLE)
                WITH (KAFKA_TOPIC='\{aggregationTable}', VALUE_FORMAT='AVRO');
                """).get();
            client.executeStatement(STR."""
                CREATE OR REPLACE STREAM \{finalStream}
                WITH (KAFKA_TOPIC='\{Topics.SUMMARY_LAST_WEEK}', VALUE_FORMAT='KAFKA')
                AS SELECT userId, amount FROM \{intermediateTableStream} PARTITION BY userId EMIT CHANGES;
                """).get();
        }
    }
}
