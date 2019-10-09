package com.jackniu.kafka.common.errors;

public class InvalidReplicaAssignmentException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidReplicaAssignmentException(String message) {
        super(message);
    }

    public InvalidReplicaAssignmentException(String message, Throwable cause) {
        super(message, cause);
    }

}
