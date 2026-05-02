package io.tfkfan.kafka.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public class PaymentRepository {
    private final JdbcTemplate jdbcTemplate;

    public PaymentRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void saveWeeklyReport(UUID key, Double amount) {
        jdbcTemplate.update("INSERT INTO weekly_summary (user_id, amount) VALUES (?, ?)", key, amount);
    }
}
