package com.hmdp.config;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Idempotent schema upgrades required by the order correctness guarantees.
 */
@Component
public class DatabaseSchemaInitializer {

    private final JdbcTemplate jdbcTemplate;

    public DatabaseSchemaInitializer(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    public void upgrade() {
        addIndexIfMissing(
                "tb_voucher_order",
                "uk_user_voucher",
                "ALTER TABLE tb_voucher_order " +
                        "ADD UNIQUE INDEX uk_user_voucher (user_id, voucher_id)"
        );
        addIndexIfMissing(
                "tb_voucher_order",
                "idx_status_create_time",
                "ALTER TABLE tb_voucher_order " +
                        "ADD INDEX idx_status_create_time (status, create_time)"
        );
    }

    private void addIndexIfMissing(String table, String index, String ddl) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.statistics " +
                        "WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?",
                Integer.class,
                table,
                index
        );
        if (count != null && count == 0) {
            jdbcTemplate.execute(ddl);
        }
    }
}
