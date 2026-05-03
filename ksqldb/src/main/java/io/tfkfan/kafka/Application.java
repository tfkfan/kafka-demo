package io.tfkfan.kafka;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Application {
    public static void main(String[] args) {
        final ClientOptions options = ClientOptions.create()
                .setHost("localhost")
                .setPort(8088);

        try (Client client = Client.create(options)) {
            final String sql = "CREATE STREAM payments_stream (userId STRING,amount DOUBLE) " +
                    "WITH (KAFKA_TOPIC='%s', VALUE_FORMAT='AVRO',  KEY='userId');"
                    + "CREATE TABLE aggregated_sums AS SELECT userId, SUM(amount) AS total_amount " +
                    "FROM payments_stream WINDOW TUMBLING (SIZE 7 DAYS) GROUP BY userId;"
                    + "CREATE STREAM emitted_changes WITH (KAFKA_TOPIC='%s', VALUE_FORMAT='DOUBLE' ) " +
                    "AS SELECT userId,amount FROM aggregated_sums EMIT CHANGES;";
            client.executeStatement(sql.formatted(Topics.PAYMENTS, Topics.SUMMARY_LAST_WEEK)).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
