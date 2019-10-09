package com.jackniu.kafka.common.errors;

public class NotEnoughReplicasException extends RetriableException {
    private static final long serialVersionUID = 1L;

    public NotEnoughReplicasException() {
        super();
    }

    public NotEnoughReplicasException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotEnoughReplicasException(String message) {
        super(message);
    }

    public NotEnoughReplicasException(Throwable cause) {
        super(cause);
    }
}
