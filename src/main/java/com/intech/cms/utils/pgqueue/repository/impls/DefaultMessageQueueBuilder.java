package com.intech.cms.utils.pgqueue.repository.impls;

import com.intech.cms.utils.pgqueue.repository.IMessageQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.sql.SQLException;

@Slf4j
public class DefaultMessageQueueBuilder implements IMessageQueue.Builder {

    private NamedParameterJdbcTemplate jdbcTemplate;
    private PlatformTransactionManager transactionManager;
    private String schema = "queues";
    private String queueName;

    @Override
    public IMessageQueue.Builder jdbcTemplate(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        return this;
    }

    @Override
    public IMessageQueue.Builder transactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        return this;
    }

    @Override
    public IMessageQueue.Builder schema(String schema) {
        this.schema = schema;
        return this;
    }

    @Override
    public IMessageQueue.Builder queueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    @Override
    public IMessageQueue build() {
        try {
            MessageQueueImpl queue = new MessageQueueImpl(jdbcTemplate, transactionManager, schema, queueName);
            queue.init();
            return queue;
        } catch (SQLException e) {
            log.error("", e);
            throw new RuntimeException(e);
        }
    }
}
