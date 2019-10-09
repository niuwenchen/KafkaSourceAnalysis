package com.jackniu.kafka.common.errors;

public class InvalidTxnTimeoutException extends ApiException {
    private static final long serialVersionUID = 1L;

    public InvalidTxnTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidTxnTimeoutException(String message) {
        super(message);
    }
}
