package com.jackniu.kafka.common.errors;

public class LeaderNotAvailableException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    public LeaderNotAvailableException(String message) {
        super(message);
    }

    public LeaderNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

}

