package com.jackniu.kafka.common.errors;

public class InvalidSessionTimeoutException extends ApiException {
    private static final long serialVersionUID = 1L;

    public InvalidSessionTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidSessionTimeoutException(String message) {
        super(message);
    }
}

