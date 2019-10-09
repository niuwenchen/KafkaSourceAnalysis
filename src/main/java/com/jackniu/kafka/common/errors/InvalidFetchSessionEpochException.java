package com.jackniu.kafka.common.errors;

public class InvalidFetchSessionEpochException extends RetriableException {
    private static final long serialVersionUID = 1L;

    public InvalidFetchSessionEpochException() {
    }

    public InvalidFetchSessionEpochException(String message) {
        super(message);
    }
}

