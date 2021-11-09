package com.intech.cms.utils.pgqueue.repository.impls;

import com.intech.cms.utils.pgqueue.repository.IMessageQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.SQLException;
import java.util.Objects;

@Slf4j
public class DefaultMessageQueueBuilder implements IMessageQueue.Builder {
    private static final String CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS %s";

    private static final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS %1$s.%2$s ( " +
            "    id          bigserial PRIMARY KEY, " +
            "    date_added  timestamp NOT NULL, " +
            "    state       smallint  NOT NULL, " +
            "    consumer_id text, " +
            "    payload     text      NOT NULL " +
            ")";

    private static final String CREATE_NOTIFY_FUNC_SQL = "CREATE OR REPLACE FUNCTION %1$s.%2$s_notify_func() RETURNS trigger AS " +
            "$$ " +
            "BEGIN " +
            "    NOTIFY %2$s; " +
            "    RETURN new; " +
            "END; " +
            "$$ LANGUAGE 'plpgsql'";

    private static final String CREATE_TRIGGER_SQL = "DO " +
            "$$ " +
            "    BEGIN " +
            "        IF NOT EXISTS(SELECT * " +
            "                      FROM information_schema.triggers " +
            "                      WHERE event_object_schema = '%1$s' " +
            "                        AND event_object_table = '%2$s' " +
            "            ) " +
            "        THEN " +
            "            CREATE TRIGGER %2$s_after_insert " +
            "                AFTER INSERT " +
            "                ON %1$s.%2$s " +
            "                FOR EACH ROW " +
            "            EXECUTE PROCEDURE %1$s.%2$s_notify_func(); " +
            "        END IF; " +
            "    END; " +
            "$$";

    private NamedParameterJdbcTemplate jdbcTemplate;
    private PlatformTransactionManager transactionManager;
    private String schema = "queues";
    private String queueName;

    @Override
    public IMessageQueue.Builder jdbcTemplate(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate);
        return this;
    }

    @Override
    public IMessageQueue.Builder transactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = Objects.requireNonNull(transactionManager);
        return this;
    }

    @Override
    public IMessageQueue.Builder schema(String schema) {
        this.schema = Objects.requireNonNull(schema);
        return this;
    }

    @Override
    public IMessageQueue.Builder queueName(String queueName) {
        this.queueName = Objects.requireNonNull(queueName);
        return this;
    }

    @Override
    public IMessageQueue build() {
        try {
            executeDDL(jdbcTemplate.getJdbcTemplate(), transactionManager);
            return new MessageQueueImpl(jdbcTemplate, transactionManager, schema, queueName);
        } catch (SQLException e) {
            log.error("", e);
            throw new RuntimeException(e);
        }
    }

    private void executeDDL(JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager) {
        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            jdbcTemplate.execute(String.format(CREATE_SCHEMA_SQL, this.schema));
            jdbcTemplate.execute(String.format(CREATE_TABLE_SQL, this.schema, this.queueName));
            jdbcTemplate.execute(String.format(CREATE_NOTIFY_FUNC_SQL, this.schema, this.queueName));
            jdbcTemplate.execute(String.format(CREATE_TRIGGER_SQL, this.schema, this.queueName));
        });
    }
}
