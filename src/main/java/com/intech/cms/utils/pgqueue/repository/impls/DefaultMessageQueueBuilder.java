package com.intech.cms.utils.pgqueue.repository.impls;

import com.intech.cms.utils.pgqueue.repository.MessageQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.Objects;

@Slf4j
public class DefaultMessageQueueBuilder implements MessageQueue.Builder {

    private NamedParameterJdbcTemplate jdbcTemplate;
    private String schema = "queues";
    private String queueName;
    private int cleanInterval = 0;//in sec
    private int cleanBatchSize = 10;
    private int messageAge = 7;//in days

    @Override
    public MessageQueue.Builder jdbcTemplate(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        return this;
    }

    @Override
    public MessageQueue.Builder schema(String schema) {
        this.schema = schema;
        return this;
    }

    @Override
    public MessageQueue.Builder queueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    @Override
    public MessageQueue.Builder enableCleaner(int interval, int batchSize, int age) {
        this.cleanInterval = interval;
        this.cleanBatchSize = batchSize;
        this.messageAge = age;
        return this;
    }

    @Override
    public MessageQueue build() {
        Objects.requireNonNull(jdbcTemplate);
        Objects.requireNonNull(schema);
        Objects.requireNonNull(queueName);
        return new MessageQueueImpl(jdbcTemplate, schema, queueName, cleanInterval, cleanBatchSize, messageAge);
    }
}
