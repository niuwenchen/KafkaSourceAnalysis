package com.jackniu.kafka.common.errors;

public class RebalanceInProgressException extends ApiException {
    private static final long serialVersionUID = 1L;

    public RebalanceInProgressException() {
        super();
    }

    public RebalanceInProgressException(String message, Throwable cause) {
        super(message, cause);
    }

    public RebalanceInProgressException(String message) {
        super(message);
    }

    public RebalanceInProgressException(Throwable cause) {
        super(cause);
    }
}

