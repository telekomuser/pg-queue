package com.intech.cms.utils.pgqueue.clients.impls;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intech.cms.utils.pgqueue.clients.DeserializationException;
import com.intech.cms.utils.pgqueue.clients.Deserializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class JsonDeserializerImpl implements Deserializer {

    private final ObjectMapper om;

    @Override
    public <T> T deserialize(String payload, Class<T> clazz) {
        try {
            return om.readValue(payload, clazz);
        } catch (JsonProcessingException e) {
            log.error("", e);
            throw new DeserializationException(e);
        }
    }
}
