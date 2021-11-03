package com.intech.cms.utils.pgqueue.repository;

import com.intech.cms.utils.pgqueue.model.Message;
import com.intech.cms.utils.pgqueue.repository.impls.DefaultMessageQueueBuilder;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

public interface IMessageQueue {
    long put(Message message);

    List<Message> findNext(String consumerId, int limit) throws InterruptedException;

    List<Message> findNextOrEmpty(String consumerId, int limit);

    void commitAllConsumedMessages(String consumerId);

    void resetAllUncommittedMessages(String consumerId);

    static IMessageQueue.Builder builder() {
        return new DefaultMessageQueueBuilder();
    }

    interface Builder {
        Builder jdbcTemplate(NamedParameterJdbcTemplate jdbcTemplate);

        Builder transactionManager(PlatformTransactionManager transactionManager);

        Builder schema(String schema);

        Builder queueName(String queueName);

        IMessageQueue build();
    }
}
