package com.intech.cms.utils.pgqueue.clients.impls;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intech.cms.utils.pgqueue.clients.SerializationException;
import com.intech.cms.utils.pgqueue.clients.Serializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class JsonSerializerImpl implements Serializer {

    private final ObjectMapper om;

    @Override
    public String serialize(Object payload) {
        try {
            return om.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            log.error("", e);
            throw new SerializationException(e);
        }
    }
}
