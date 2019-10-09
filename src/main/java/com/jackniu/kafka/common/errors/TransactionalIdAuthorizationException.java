package com.jackniu.kafka.common.errors;

public class TransactionalIdAuthorizationException extends AuthorizationException {
    public TransactionalIdAuthorizationException(final String message) {
        super(message);
    }
}
