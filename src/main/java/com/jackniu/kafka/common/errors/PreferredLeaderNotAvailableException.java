package com.jackniu.kafka.common.errors;

public class PreferredLeaderNotAvailableException extends InvalidMetadataException {

    public PreferredLeaderNotAvailableException(String message) {
        super(message);
    }

    public PreferredLeaderNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}

