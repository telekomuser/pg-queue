package com.intech.cms.utils.pgqueue.model;

import java.util.Arrays;

public enum MessageState {
    ACCEPTED(1),
    SENT(2),
    DELIVERED(3);

    private final int id;

    MessageState(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static MessageState getById(int id) {
        return Arrays.stream(values())
                .filter(value -> value.id == id)
                .findFirst()
                .orElseThrow(IllegalArgumentException::new);
    }
}
