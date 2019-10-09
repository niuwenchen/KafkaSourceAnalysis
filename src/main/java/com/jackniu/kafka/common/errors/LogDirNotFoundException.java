package com.jackniu.kafka.common.errors;

public class LogDirNotFoundException extends ApiException {

    private static final long serialVersionUID = 1L;

    public LogDirNotFoundException(String message) {
        super(message);
    }

    public LogDirNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public LogDirNotFoundException(Throwable cause) {
        super(cause);
    }
}
