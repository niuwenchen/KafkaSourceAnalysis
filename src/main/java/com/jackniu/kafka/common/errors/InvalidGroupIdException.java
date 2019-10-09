package com.jackniu.kafka.common.errors;

public class InvalidGroupIdException extends ApiException {
    private static final long serialVersionUID = 1L;

    public InvalidGroupIdException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidGroupIdException(String message) {
        super(message);
    }
}
