package com.jackniu.kafka.common.errors;

public class SaslAuthenticationException extends AuthenticationException {

    private static final long serialVersionUID = 1L;

    public SaslAuthenticationException(String message) {
        super(message);
    }

    public SaslAuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

}
