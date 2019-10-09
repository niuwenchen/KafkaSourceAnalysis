package com.jackniu.kafka.common.errors;

public class DelegationTokenOwnerMismatchException extends ApiException {

    private static final long serialVersionUID = 1L;

    public DelegationTokenOwnerMismatchException(String message) {
        super(message);
    }

    public DelegationTokenOwnerMismatchException(String message, Throwable cause) {
        super(message, cause);
    }

}

