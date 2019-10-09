package com.jackniu.kafka.common.errors;

public class DelegationTokenExpiredException extends ApiException {

    private static final long serialVersionUID = 1L;

    public DelegationTokenExpiredException(String message) {
        super(message);
    }

    public DelegationTokenExpiredException(String message, Throwable cause) {
        super(message, cause);
    }

}

