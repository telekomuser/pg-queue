package com.intech.cms.utils.pgqueue.model;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class Message {
    private final Long offset;//as ID
    private final LocalDateTime dateAdded;
    private final MessageState state;
    private final String consumerId;
    private final LocalDateTime stateUpdatedAt;
    private final String payload;
}
