package com.jackniu.kafka.common.errors;

public class FencedLeaderEpochException extends InvalidMetadataException {
    private static final long serialVersionUID = 1L;

    public FencedLeaderEpochException(String message) {
        super(message);
    }

    public FencedLeaderEpochException(String message, Throwable cause) {
        super(message, cause);
    }

}
