package com.jackniu.kafka.common.errors;

public class UnsupportedVersionException extends ApiException {
    private static final long serialVersionUID = 1L;

    public UnsupportedVersionException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnsupportedVersionException(String message) {
        super(message);
    }
}
