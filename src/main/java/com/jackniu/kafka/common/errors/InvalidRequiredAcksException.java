package com.jackniu.kafka.common.errors;

public class InvalidRequiredAcksException extends ApiException {
    private static final long serialVersionUID = 1L;

    public InvalidRequiredAcksException(String message) {
        super(message);
    }
}
