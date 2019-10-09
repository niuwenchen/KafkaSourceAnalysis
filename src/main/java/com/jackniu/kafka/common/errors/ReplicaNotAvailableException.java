package com.jackniu.kafka.common.errors;

public class ReplicaNotAvailableException extends ApiException {

    private static final long serialVersionUID = 1L;

    public ReplicaNotAvailableException(String message) {
        super(message);
    }

    public ReplicaNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReplicaNotAvailableException(Throwable cause) {
        super(cause);
    }

}

