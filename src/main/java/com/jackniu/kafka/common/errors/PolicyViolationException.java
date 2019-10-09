package com.jackniu.kafka.common.errors;

public class PolicyViolationException extends ApiException {

    public PolicyViolationException(String message) {
        super(message);
    }

    public PolicyViolationException(String message, Throwable cause) {
        super(message, cause);
    }
}

