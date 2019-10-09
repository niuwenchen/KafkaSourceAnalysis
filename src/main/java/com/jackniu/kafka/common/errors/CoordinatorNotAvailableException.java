package com.jackniu.kafka.common.errors;

public class CoordinatorNotAvailableException extends RetriableException {
    public static final CoordinatorNotAvailableException INSTANCE = new CoordinatorNotAvailableException();

    private static final long serialVersionUID = 1L;

    private CoordinatorNotAvailableException() {
        super();
    }

    public CoordinatorNotAvailableException(String message) {
        super(message);
    }

    public CoordinatorNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

}
