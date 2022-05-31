package com.intech.cms.utils.pgqueue.clients.impls;

import com.intech.cms.utils.pgqueue.clients.MessageProducer;
import com.intech.cms.utils.pgqueue.clients.Serializer;
import com.intech.cms.utils.pgqueue.repository.MessageQueue;

public class MessageProducerImpl implements MessageProducer {

    private final Serializer serializer;
    private final MessageQueue messageQueue;

    protected MessageProducerImpl(Serializer serializer, MessageQueue messageQueue) {
        this.serializer = serializer;
        this.messageQueue = messageQueue;
    }

    @Override
    public <T> long send(T payload) {
        return messageQueue.put(serializer.serialize(payload)).getOffset();
    }
}
