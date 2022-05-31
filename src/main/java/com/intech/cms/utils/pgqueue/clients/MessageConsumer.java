package com.intech.cms.utils.pgqueue.clients;

import com.intech.cms.utils.pgqueue.clients.impls.DefaultMessageConsumerBuilder;
import com.intech.cms.utils.pgqueue.repository.MessageQueue;

import java.util.List;

public interface MessageConsumer<T> extends AutoCloseable {

    T takeOne(String consumerId) throws InterruptedException;

    List<T> take(String consumerId, int limit) throws InterruptedException;

    List<T> takeIfPresent(String consumerId, int limit);

    void reset(String consumerId);

    void commit(String consumerId);

    static <R> MessageConsumer.Builder<R> builder() {
        return new DefaultMessageConsumerBuilder<>();
    }

    interface Builder<T> {
        MessageConsumer.Builder<T> messageQueue(MessageQueue messageQueue);

        MessageConsumer.Builder<T> deserialize(Deserializer deserializer);

        MessageConsumer.Builder<T> clazz(Class<T> clazz);

        MessageConsumer.Builder<T> pollInterval(int pollInterval);

        MessageConsumer.Builder<T> enableRestorer(int interval, int limit, int age);

        MessageConsumer<T> build();
    }

}
