package com.intech.cms.utils.pgqueue.clients.impls;

import com.intech.cms.utils.pgqueue.clients.MessageProducer;
import com.intech.cms.utils.pgqueue.clients.Serializer;
import com.intech.cms.utils.pgqueue.repository.MessageQueue;

import java.util.Objects;

public class DefaultMessageProducerBuilder implements MessageProducer.Builder {

    private Serializer serializer;
    private MessageQueue messageQueue;

    @Override
    public MessageProducer.Builder serializer(Serializer serializer) {
        this.serializer = serializer;
        return this;
    }

    @Override
    public MessageProducer.Builder messageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
        return this;
    }

    @Override
    public MessageProducer build() {
        Objects.requireNonNull(serializer);
        Objects.requireNonNull(messageQueue);
        return new MessageProducerImpl(serializer, messageQueue);
    }
}
