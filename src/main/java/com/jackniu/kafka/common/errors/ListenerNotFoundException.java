package com.jackniu.kafka.common.errors;

public class ListenerNotFoundException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    public ListenerNotFoundException(String message) {
        super(message);
    }

    public ListenerNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

}
