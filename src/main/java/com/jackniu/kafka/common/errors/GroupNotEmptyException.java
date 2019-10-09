package com.jackniu.kafka.common.errors;

public class GroupNotEmptyException extends ApiException {
    public GroupNotEmptyException(String message) {
        super(message);
    }
}

