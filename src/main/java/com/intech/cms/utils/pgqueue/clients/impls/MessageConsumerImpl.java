package com.intech.cms.utils.pgqueue.clients.impls;

import com.intech.cms.utils.pgqueue.clients.IDeserializer;
import com.intech.cms.utils.pgqueue.clients.IMessageConsumer;
import com.intech.cms.utils.pgqueue.repository.IMessageQueue;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class MessageConsumerImpl<T> implements IMessageConsumer<T> {

    private final IMessageQueue queueRepository;
    private final IDeserializer deserializer;
    private final Class<T> clazz;

    public MessageConsumerImpl(IMessageQueue queueRepository,
                               IDeserializer deserializer,
                               Class<T> clazz) {
        this.queueRepository = queueRepository;
        this.deserializer = deserializer;
        this.clazz = clazz;
    }

    @Override
    public Stream<T> takeIfPresent(String consumerId, int limit) {
        return queueRepository.findNextOrEmpty(consumerId, limit).stream()
                .map(message -> deserializer.deserialize(message.getPayload(), clazz));
    }

    @Override
    public T takeOne(String consumerId) throws InterruptedException {
        return queueRepository.findNext(consumerId, 1).stream()
                .map(message -> deserializer.deserialize(message.getPayload(), clazz))
                .findFirst()
                .orElseThrow(IllegalStateException::new);
    }

    @Override
    public List<T> take(String consumerId, int limit) throws InterruptedException {
        return queueRepository.findNext(consumerId, limit).stream()
                .map(message -> deserializer.deserialize(message.getPayload(), clazz))
                .collect(Collectors.toList());
    }

    @Override
    public void reset(String consumerId) {
        queueRepository.resetAllUncommittedMessages(consumerId);
    }

    @Override
    public void commit(String consumerId) {
        queueRepository.commitAllConsumedMessages(consumerId);
    }

}