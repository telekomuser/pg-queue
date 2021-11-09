package com.intech.cms.utils.pgqueue.model;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class Message {
    private final Long offset;//as ID

    private final LocalDateTime dateAdded;

    @Builder.Default
    private final MessageState state = MessageState.ACCEPTED;

    private final String consumerId;

    private final String payload;
}
