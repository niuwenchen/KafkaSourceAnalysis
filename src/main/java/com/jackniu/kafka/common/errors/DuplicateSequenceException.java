package com.jackniu.kafka.common.errors;

public class DuplicateSequenceException  extends ApiException {

    public DuplicateSequenceException(String message) {
        super(message);
    }
}

