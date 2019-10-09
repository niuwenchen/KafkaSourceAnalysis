package com.jackniu.kafka.common.errors;

public class IllegalGenerationException extends ApiException {
    private static final long serialVersionUID = 1L;

    public IllegalGenerationException() {
        super();
    }

    public IllegalGenerationException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalGenerationException(String message) {
        super(message);
    }

    public IllegalGenerationException(Throwable cause) {
        super(cause);
    }
}

