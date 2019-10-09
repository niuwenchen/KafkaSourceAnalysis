package com.jackniu.kafka.common.errors;

public class DelegationTokenNotFoundException extends ApiException {

    private static final long serialVersionUID = 1L;

    public DelegationTokenNotFoundException(String message) {
        super(message);
    }

    public DelegationTokenNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

}