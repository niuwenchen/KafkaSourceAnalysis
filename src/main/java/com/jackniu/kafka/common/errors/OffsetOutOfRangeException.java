package com.jackniu.kafka.common.errors;

public class OffsetOutOfRangeException extends InvalidOffsetException {

    private static final long serialVersionUID = 1L;

    public OffsetOutOfRangeException(String message) {
        super(message);
    }

    public OffsetOutOfRangeException(String message, Throwable cause) {
        super(message, cause);
    }

}

