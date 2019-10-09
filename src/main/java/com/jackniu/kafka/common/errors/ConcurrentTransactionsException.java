package com.jackniu.kafka.common.errors;

public class ConcurrentTransactionsException extends ApiException {
    private static final long serialVersionUID = 1L;

    public ConcurrentTransactionsException(final String message) {
        super(message);
    }
}

