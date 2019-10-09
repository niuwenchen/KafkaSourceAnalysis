package com.jackniu.kafka.common.errors;

public class BrokerNotAvailableException extends ApiException {

    private static final long serialVersionUID = 1L;

    public BrokerNotAvailableException(String message) {
        super(message);
    }

    public BrokerNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

}
