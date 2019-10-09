package com.jackniu.kafka.common.errors;

public class DelegationTokenAuthorizationException extends AuthorizationException {

    private static final long serialVersionUID = 1L;

    public DelegationTokenAuthorizationException(String message) {
        super(message);
    }

    public DelegationTokenAuthorizationException(String message, Throwable cause) {
        super(message, cause);
    }

}
