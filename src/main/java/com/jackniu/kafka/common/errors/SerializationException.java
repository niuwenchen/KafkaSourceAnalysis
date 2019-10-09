package com.jackniu.kafka.common.errors;

import com.jackniu.kafka.common.KafkaException;

public class SerializationException extends KafkaException {

    private static final long serialVersionUID = 1L;

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(Throwable cause) {
        super(cause);
    }

    public SerializationException() {
        super();
    }

    /* avoid the expensive and useless stack trace for serialization exceptions */
    @Override
    public Throwable fillInStackTrace() {
        return this;
    }


}
