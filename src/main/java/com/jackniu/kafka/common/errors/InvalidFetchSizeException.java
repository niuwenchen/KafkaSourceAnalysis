package com.jackniu.kafka.common.errors;

public class InvalidFetchSizeException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidFetchSizeException(String message) {
        super(message);
    }

    public InvalidFetchSizeException(String message, Throwable cause) {
        super(message, cause);
    }

}

