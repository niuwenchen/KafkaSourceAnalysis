package com.jackniu.kafka.common.errors;

public class InvalidRequestException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidRequestException(String message) {
        super(message);
    }

    public InvalidRequestException(String message, Throwable cause) {
        super(message, cause);
    }

}

