package com.jackniu.kafka.common.errors;

public class ReassignmentInProgressException  extends ApiException {

    public ReassignmentInProgressException(String msg) {
        super(msg);
    }

    public ReassignmentInProgressException(String msg, Throwable cause) {
        super(msg, cause);
    }
}

