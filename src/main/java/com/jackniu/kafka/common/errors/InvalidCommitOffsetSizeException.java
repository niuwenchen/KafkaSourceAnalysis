package com.jackniu.kafka.common.errors;

public class InvalidCommitOffsetSizeException extends ApiException {
    private static final long serialVersionUID = 1L;

    public InvalidCommitOffsetSizeException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidCommitOffsetSizeException(String message) {
        super(message);
    }
}

