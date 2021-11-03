package com.intech.cms.utils.pgqueue.clients;

public interface IMessageProducer {

    <T> long send(T payload);

}
