package com.jackniu.kafka.common.errors;

public class DisconnectException extends RetriableException {
    public static final DisconnectException INSTANCE = new DisconnectException();

    private static final long serialVersionUID = 1L;

    public DisconnectException() {
        super();
    }

    public DisconnectException(String message, Throwable cause) {
        super(message, cause);
    }

    public DisconnectException(String message) {
        super(message);
    }

    public DisconnectException(Throwable cause) {
        super(cause);
    }

}
