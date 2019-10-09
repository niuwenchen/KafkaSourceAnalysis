package com.jackniu.kafka.common.errors;

public class OffsetNotAvailableException extends RetriableException {
    private static final long serialVersionUID = 1L;

    public OffsetNotAvailableException(String message) {
        super(message);
    }
}
