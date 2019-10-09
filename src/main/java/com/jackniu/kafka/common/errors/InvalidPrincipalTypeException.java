package com.jackniu.kafka.common.errors;

public class InvalidPrincipalTypeException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidPrincipalTypeException(String message) {
        super(message);
    }

    public InvalidPrincipalTypeException(String message, Throwable cause) {
        super(message, cause);
    }

}

