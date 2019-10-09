package com.jackniu.kafka.common.errors;

public class ProducerFencedException extends ApiException {
    public ProducerFencedException(String msg) {
        super(msg);
    }
}
