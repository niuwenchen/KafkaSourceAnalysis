package com.jackniu.kafka.common.errors;

import com.jackniu.kafka.common.KafkaException;

public class InterruptException extends KafkaException {

    private static final long serialVersionUID = 1L;

    public InterruptException(InterruptedException cause) {
        super(cause);
        Thread.currentThread().interrupt();
    }

    public InterruptException(String message, InterruptedException cause) {
        super(message, cause);
        Thread.currentThread().interrupt();
    }

    public InterruptException(String message) {
        super(message, new InterruptedException());
        Thread.currentThread().interrupt();
    }

}
