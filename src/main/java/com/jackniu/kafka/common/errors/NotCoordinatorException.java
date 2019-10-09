package com.jackniu.kafka.common.errors;

public class NotCoordinatorException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public NotCoordinatorException(String message) {
        super(message);
    }

    public NotCoordinatorException(String message, Throwable cause) {
        super(message, cause);
    }

}
