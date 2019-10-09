package com.jackniu.kafka.common.network;

import com.jackniu.kafka.common.errors.AuthenticationException;

public class DelayedResponseAuthenticationException extends AuthenticationException {
    private static final long serialVersionUID = 1L;

    public DelayedResponseAuthenticationException(Throwable cause) {
        super(cause);
    }
}

