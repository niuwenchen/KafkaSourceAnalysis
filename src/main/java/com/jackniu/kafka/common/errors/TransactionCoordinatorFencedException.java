package com.jackniu.kafka.common.errors;

public class TransactionCoordinatorFencedException extends ApiException {

    private static final long serialVersionUID = 1L;

    public TransactionCoordinatorFencedException(String message) {
        super(message);
    }

    public TransactionCoordinatorFencedException(String message, Throwable cause) {
        super(message, cause);
    }
}

