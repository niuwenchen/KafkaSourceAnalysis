package com.jackniu.kafka.common.errors;

public class DelegationTokenDisabledException extends ApiException {

    private static final long serialVersionUID = 1L;

    public DelegationTokenDisabledException(String message) {
        super(message);
    }

    public DelegationTokenDisabledException(String message, Throwable cause) {
        super(message, cause);
    }

}

