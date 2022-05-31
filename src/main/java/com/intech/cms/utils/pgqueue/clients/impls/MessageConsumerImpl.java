package com.intech.cms.utils.pgqueue.clients.impls;

import com.intech.cms.utils.pgqueue.clients.Deserializer;
import com.intech.cms.utils.pgqueue.clients.MessageConsumer;
import com.intech.cms.utils.pgqueue.model.Message;
import com.intech.cms.utils.pgqueue.model.MessageState;
import com.intech.cms.utils.pgqueue.repository.MessageQueue;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Slf4j
public class MessageConsumerImpl<T> implements MessageConsumer<T> {

    private final MessageQueue queue;
    private final Deserializer deserializer;
    private final Class<T> clazz;

    private final Lock lock;
    private final Condition newMessageEvent;
    private final Poller poller;
    private final Optional<Restorer> restorer;

    protected MessageConsumerImpl(MessageQueue queue,
                                  Deserializer deserializer,
                                  Class<T> clazz,
                                  int pollInterval,
                                  int restoreInterval,
                                  int restoreLimit,
                                  int restoreAge) {
        this.queue = queue;
        this.deserializer = deserializer;
        this.clazz = clazz;
        this.lock = new ReentrantLock();
        this.newMessageEvent = this.lock.newCondition();
        this.poller = new Poller(String.format("%s-poller", queue.getName()), pollInterval);
        this.poller.start();

        this.restorer = Optional.of(restoreInterval)
                .filter(interval -> interval > 0)
                .map(interval ->
                        new Restorer(
                                String.format("%s-restore", queue.getName()),
                                interval,
                                restoreLimit,
                                restoreAge)
                );
        this.restorer.ifPresent(Restorer::start);
    }

    @Override
    public List<T> takeIfPresent(String consumerId, int limit) {
        return selectNewIfExists(consumerId, limit).stream()
                .map(message -> deserializer.deserialize(message.getPayload(), clazz))
                .collect(Collectors.toList());
    }

    @Override
    public T takeOne(String consumerId) throws InterruptedException {
        List<Message> messages;

        while ((messages = selectNewIfExists(consumerId, 1)).isEmpty()) {
            try {
                lock.lock();
                log.debug("awaiting for a new messages...");
                newMessageEvent.await();
            } finally {
                lock.unlock();
            }
        }

        return messages.stream()
                .map(message -> deserializer.deserialize(message.getPayload(), clazz))
                .findFirst()
                .orElseThrow(IllegalStateException::new);
    }

    @Override
    public List<T> take(String consumerId, int limit) throws InterruptedException {
        List<Message> messages;

        while ((messages = selectNewIfExists(consumerId, limit)).isEmpty()) {
            try {
                lock.lock();
                log.debug("awaiting for a new messages...");
                newMessageEvent.await();
            } finally {
                lock.unlock();
            }
        }

        return messages.stream()
                .map(message -> deserializer.deserialize(message.getPayload(), clazz))
                .collect(Collectors.toList());
    }

    @Override
    public void reset(String consumerId) {
        queue.updateStateByConsumerId(consumerId, MessageState.SENT, MessageState.ACCEPTED);
    }

    @Override
    public void commit(String consumerId) {
        queue.updateStateByConsumerId(consumerId, MessageState.SENT, MessageState.DELIVERED);
    }

    @Override
    public void close() {
        poller.interrupt();
        restorer.ifPresent(Restorer::interrupt);
    }

    private List<Message> selectNewIfExists(String consumerId, int limit) {
        return queue.selectByStateAndUpdate(MessageState.ACCEPTED, limit, MessageState.SENT, consumerId);
    }

    private class Poller extends Thread {

        private final int pollInterval;

        public Poller(String name, int pollInterval) {
            super(name);
            this.pollInterval = pollInterval;
        }

        @Override
        public void run() {
            log.info("poller started...");

            while (!isInterrupted()) {
                try {
                    Thread.sleep(pollInterval * 1000L);

                    if (queue.isMessagesExistsForState(MessageState.ACCEPTED)) {
                        lock.lock();
                        newMessageEvent.signalAll();
                        lock.unlock();
                    }
                } catch (InterruptedException e) {
                    log.info("poller interrupted, stopping...");
                    break;
                } catch (Exception e) {
                    log.error("", e);
                }
            }

            log.info("...poller stopped");
        }
    }

    private class Restorer extends Thread {

        private final int interval;
        private final int limit;
        private final int age;

        public Restorer(String name, int interval, int limit, int age) {
            super(name);
            this.interval = interval;
            this.limit = limit;
            this.age = age;
        }

        @Override
        public void run() {
            log.info("restorer started...");

            while (!isInterrupted()) {
                try {
                    Thread.sleep(interval * 1000L);
                    int restored = queue.updateStateOlderThan(
                            MessageState.SENT,
                            MessageState.ACCEPTED,
                            limit,
                            LocalDateTime.now().minusHours(age)
                    );
                    log.info("{} messages restored", restored);
                } catch (InterruptedException e) {
                    log.info("restorer interrupted, stopping...");
                    break;
                } catch (Exception e) {
                    log.error("", e);
                }
            }

            log.info("...restorer stopped");
        }
    }

}