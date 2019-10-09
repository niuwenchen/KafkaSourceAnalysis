package com.jackniu.kafka.common.errors;

public class CoordinatorLoadInProgressException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public CoordinatorLoadInProgressException(String message) {
        super(message);
    }

    public CoordinatorLoadInProgressException(String message, Throwable cause) {
        super(message, cause);
    }

}