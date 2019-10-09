package com.jackniu.kafka.common.errors;

public class OutOfOrderSequenceException extends ApiException {

    public OutOfOrderSequenceException(String msg) {
        super(msg);
    }
}

