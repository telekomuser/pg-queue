package com.intech.cms.utils.pgqueue.clients;

import java.util.List;
import java.util.stream.Stream;

public interface IMessageConsumer<T> {

    T takeOne(String consumerId) throws InterruptedException;

    List<T> take(String consumerId, int limit) throws InterruptedException;

    Stream<T> takeIfPresent(String consumerId, int limit);

    void reset(String consumerId);

    void commit(String consumerId);
}
