package com.jackniu.kafka.common.errors;

public class UnsupportedSaslMechanismException extends AuthenticationException {

    private static final long serialVersionUID = 1L;

    public UnsupportedSaslMechanismException(String message) {
        super(message);
    }

    public UnsupportedSaslMechanismException(String message, Throwable cause) {
        super(message, cause);
    }

}
