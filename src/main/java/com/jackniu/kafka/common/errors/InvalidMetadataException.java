package com.jackniu.kafka.common.errors;

public abstract class InvalidMetadataException extends RetriableException {
    private static final long serialVersionUID = 1L;

    public InvalidMetadataException() {
        super();
    }

    public InvalidMetadataException(String message) {
        super(message);
    }

    public InvalidMetadataException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidMetadataException(Throwable cause) {
        super(cause);
    }
}
