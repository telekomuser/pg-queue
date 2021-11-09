package com.intech.cms.utils.pgqueue.clients.impls;

import com.intech.cms.utils.pgqueue.clients.IMessageProducer;
import com.intech.cms.utils.pgqueue.clients.ISerializer;
import com.intech.cms.utils.pgqueue.model.Message;
import com.intech.cms.utils.pgqueue.repository.IMessageQueue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class MessageProducerImpl implements IMessageProducer {

    private final ISerializer serializer;
    private final IMessageQueue messageQueue;

    @Override
    public <T> long send(T payload) {
        Message message = Message.builder()
                .payload(serializer.serialize(payload))
                .build();
        return messageQueue.put(message).getOffset();
    }
}
