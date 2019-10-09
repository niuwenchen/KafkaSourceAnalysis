package com.jackniu.kafka.common.errors;

public class IllegalSaslStateException extends AuthenticationException {

    private static final long serialVersionUID = 1L;

    public IllegalSaslStateException(String message) {
        super(message);
    }

    public IllegalSaslStateException(String message, Throwable cause) {
        super(message, cause);
    }

}
