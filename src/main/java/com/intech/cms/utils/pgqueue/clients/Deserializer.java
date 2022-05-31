package com.intech.cms.utils.pgqueue.clients;

public interface Deserializer {

    <T> T deserialize(String payload, Class<T> clazz);

}
