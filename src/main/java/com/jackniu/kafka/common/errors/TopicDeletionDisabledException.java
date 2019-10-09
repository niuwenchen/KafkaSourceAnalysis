package com.jackniu.kafka.common.errors;

public class TopicDeletionDisabledException extends  ApiException {
    private static final long serialVersionUID = 1L;

    public TopicDeletionDisabledException() {
    }

    public TopicDeletionDisabledException(String message) {
        super(message);
    }
}
