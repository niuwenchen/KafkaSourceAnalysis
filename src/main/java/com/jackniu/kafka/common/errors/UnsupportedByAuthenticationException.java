package com.jackniu.kafka.common.errors;

public class UnsupportedByAuthenticationException extends ApiException {
    private static final long serialVersionUID = 1L;

    public UnsupportedByAuthenticationException(String message) {
        super(message);
    }

    public UnsupportedByAuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

}

