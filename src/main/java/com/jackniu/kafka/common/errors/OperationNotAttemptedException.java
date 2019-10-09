package com.jackniu.kafka.common.errors;

public class OperationNotAttemptedException  extends ApiException {
    public OperationNotAttemptedException(final String message) {
        super(message);
    }
}

