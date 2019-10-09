package com.jackniu.kafka.common.errors;

public class InvalidPartitionsException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidPartitionsException(String message) {
        super(message);
    }

    public InvalidPartitionsException(String message, Throwable cause) {
        super(message, cause);
    }

}
