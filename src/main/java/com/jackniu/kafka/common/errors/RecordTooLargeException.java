package com.jackniu.kafka.common.errors;

import com.jackniu.kafka.common.TopicPartition;

import java.util.Map;

public class RecordTooLargeException extends ApiException {

    private static final long serialVersionUID = 1L;
    private Map<TopicPartition, Long> recordTooLargePartitions = null;

    public RecordTooLargeException() {
        super();
    }

    public RecordTooLargeException(String message, Throwable cause) {
        super(message, cause);
    }

    public RecordTooLargeException(String message) {
        super(message);
    }

    public RecordTooLargeException(Throwable cause) {
        super(cause);
    }

    public RecordTooLargeException(String message, Map<TopicPartition, Long> recordTooLargePartitions) {
        super(message);
        this.recordTooLargePartitions = recordTooLargePartitions;
    }

    public Map<TopicPartition, Long> recordTooLargePartitions() {
        return recordTooLargePartitions;
    }
}

