package com.intech.cms.utils.pgqueue.clients;

import com.intech.cms.utils.pgqueue.clients.impls.DefaultMessageProducerBuilder;
import com.intech.cms.utils.pgqueue.repository.MessageQueue;

public interface MessageProducer {

    <T> long send(T payload);

    static MessageProducer.Builder builder() {
        return new DefaultMessageProducerBuilder();
    }

    interface Builder {
        MessageProducer.Builder serializer(Serializer serializer);

        MessageProducer.Builder messageQueue(MessageQueue messageQueue);

        MessageProducer build();
    }

}
