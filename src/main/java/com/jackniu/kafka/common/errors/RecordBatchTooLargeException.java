package com.jackniu.kafka.common.errors;

public class RecordBatchTooLargeException extends ApiException {

    private static final long serialVersionUID = 1L;

    public RecordBatchTooLargeException() {
        super();
    }

    public RecordBatchTooLargeException(String message, Throwable cause) {
        super(message, cause);
    }

    public RecordBatchTooLargeException(String message) {
        super(message);
    }

    public RecordBatchTooLargeException(Throwable cause) {
        super(cause);
    }

}
