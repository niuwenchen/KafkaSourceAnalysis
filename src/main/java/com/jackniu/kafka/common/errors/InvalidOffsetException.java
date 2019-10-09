package com.jackniu.kafka.common.errors;

public class InvalidOffsetException  extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidOffsetException(String message) {
        super(message);
    }

    public InvalidOffsetException(String message, Throwable cause) {
        super(message, cause);
    }

}
