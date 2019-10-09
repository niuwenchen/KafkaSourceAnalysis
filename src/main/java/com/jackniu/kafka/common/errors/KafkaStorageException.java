package com.jackniu.kafka.common.errors;

public class KafkaStorageException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    public KafkaStorageException() {
        super();
    }

    public KafkaStorageException(String message) {
        super(message);
    }

    public KafkaStorageException(Throwable cause) {
        super(cause);
    }

    public KafkaStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
