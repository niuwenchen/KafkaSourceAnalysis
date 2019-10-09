package com.jackniu.kafka.common.errors;

public class SecurityDisabledException extends ApiException {
    private static final long serialVersionUID = 1L;

    public SecurityDisabledException(String message) {
        super(message);
    }

    public SecurityDisabledException(String message, Throwable cause) {
        super(message, cause);
    }
}

