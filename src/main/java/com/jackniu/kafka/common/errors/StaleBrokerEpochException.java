package com.jackniu.kafka.common.errors;

public class StaleBrokerEpochException extends ApiException {

    private static final long serialVersionUID = 1L;

    public StaleBrokerEpochException(String message) {
        super(message);
    }

    public StaleBrokerEpochException(String message, Throwable cause) {
        super(message, cause);
    }

}
