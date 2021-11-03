package com.intech.cms.utils.pgqueue.clients;

public interface IDeserializer {

    <T> T deserialize(String payload, Class<T> clazz);

}
