package com.jackniu.kafka.common.errors;

public class UnknownMemberIdException extends ApiException {
    private static final long serialVersionUID = 1L;

    public UnknownMemberIdException() {
        super();
    }

    public UnknownMemberIdException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnknownMemberIdException(String message) {
        super(message);
    }

    public UnknownMemberIdException(Throwable cause) {
        super(cause);
    }
}
