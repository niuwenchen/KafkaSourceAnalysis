package com.jackniu.kafka.common.errors;

public class UnknownProducerIdException extends OutOfOrderSequenceException {

    public UnknownProducerIdException(String message) {
        super(message);
    }

}
