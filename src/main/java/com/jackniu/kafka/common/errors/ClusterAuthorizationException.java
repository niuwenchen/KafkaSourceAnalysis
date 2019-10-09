package com.jackniu.kafka.common.errors;

public class ClusterAuthorizationException extends AuthorizationException {

    private static final long serialVersionUID = 1L;

    public ClusterAuthorizationException(String message) {
        super(message);
    }

    public ClusterAuthorizationException(String message, Throwable cause) {
        super(message, cause);
    }
}

