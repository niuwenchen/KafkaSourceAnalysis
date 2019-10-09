package com.jackniu.kafka.common.errors;

public class InconsistentGroupProtocolException extends ApiException {
    private static final long serialVersionUID = 1L;

    public InconsistentGroupProtocolException(String message, Throwable cause) {
        super(message, cause);
    }

    public InconsistentGroupProtocolException(String message) {
        super(message);
    }
}

