package com.jackniu.kafka.common.errors;

public class SslAuthenticationException extends AuthenticationException {

    private static final long serialVersionUID = 1L;

    public SslAuthenticationException(String message) {
        super(message);
    }

    public SslAuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

}
