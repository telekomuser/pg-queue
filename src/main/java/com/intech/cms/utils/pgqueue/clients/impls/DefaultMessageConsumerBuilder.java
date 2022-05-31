package com.intech.cms.utils.pgqueue.clients.impls;

import com.intech.cms.utils.pgqueue.clients.Deserializer;
import com.intech.cms.utils.pgqueue.clients.MessageConsumer;
import com.intech.cms.utils.pgqueue.repository.MessageQueue;

import java.util.Objects;

public class DefaultMessageConsumerBuilder<T> implements MessageConsumer.Builder<T> {

    private MessageQueue queue;
    private Deserializer deserializer;
    private Class<T> clazz;
    private int pollInterval = 2;//in sec
    private int restoreInterval = 0;
    private int restoreLimit = 10;
    private int restoreAge = 3;

    @Override
    public MessageConsumer.Builder<T> messageQueue(MessageQueue queue) {
        this.queue = queue;
        return this;
    }

    @Override
    public MessageConsumer.Builder<T> deserialize(Deserializer deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    @Override
    public MessageConsumer.Builder<T> clazz(Class<T> clazz) {
        this.clazz = clazz;
        return this;
    }

    @Override
    public MessageConsumer.Builder<T> pollInterval(int pollInterval) {
        this.pollInterval = pollInterval;
        return this;
    }

    @Override
    public MessageConsumer.Builder<T> enableRestorer(int interval, int limit, int age) {
        this.restoreInterval = interval;
        this.restoreLimit = limit;
        this.restoreAge = age;
        return this;
    }

    @Override
    public MessageConsumer<T> build() {
        Objects.requireNonNull(queue);
        Objects.requireNonNull(deserializer);
        Objects.requireNonNull(clazz);
        if (pollInterval < 2 || pollInterval > 300) {
            throw new IllegalArgumentException("poll interval must be between 2 and 300 seconds");
        }
        return new MessageConsumerImpl<>(queue, deserializer, clazz, pollInterval, restoreInterval, restoreLimit, restoreAge);
    }
}
