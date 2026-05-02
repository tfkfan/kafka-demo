package io.tfkfan.kafka.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class PaymentRepository {
    private final JdbcTemplate jdbcTemplate;

    public PaymentRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void saveWeeklyReport(String key, Double amount) {
        jdbcTemplate.update("INSERT INTO weekly_summary (user_id, amount) VALUES (?, ?) " +
                "ON CONFLICT (user_id) DO UPDATE " +
                "SET amount = EXCLUDED.amount;",
                key, amount);
    }
}
