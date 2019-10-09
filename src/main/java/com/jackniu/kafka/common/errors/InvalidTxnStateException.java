package com.jackniu.kafka.common.errors;

public class InvalidTxnStateException extends ApiException {
    public InvalidTxnStateException(String message) {
        super(message);
    }
}
