package com.jackniu.kafka.common.errors;

public class FetchSessionIdNotFoundException extends RetriableException {
    private static final long serialVersionUID = 1L;

    public FetchSessionIdNotFoundException() {
    }

    public FetchSessionIdNotFoundException(String message) {
        super(message);
    }
}

