package com.jackniu.kafka.common.record;

import com.jackniu.kafka.common.errors.CorruptRecordException;

public class InvalidRecordException extends CorruptRecordException {

    private static final long serialVersionUID = 1;

    public InvalidRecordException(String s) {
        super(s);
    }

    public InvalidRecordException(String message, Throwable cause) {
        super(message, cause);
    }

}
