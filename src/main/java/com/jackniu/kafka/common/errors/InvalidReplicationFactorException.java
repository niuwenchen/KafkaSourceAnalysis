package com.jackniu.kafka.common.errors;

public class InvalidReplicationFactorException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidReplicationFactorException(String message) {
        super(message);
    }

    public InvalidReplicationFactorException(String message, Throwable cause) {
        super(message, cause);
    }

}
