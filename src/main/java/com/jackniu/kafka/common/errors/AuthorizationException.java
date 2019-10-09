package com.jackniu.kafka.common.errors;

public class AuthorizationException extends ApiException {

    public AuthorizationException(String message) {
        super(message);
    }

    public AuthorizationException(String message, Throwable cause) {
        super(message, cause);
    }

}

