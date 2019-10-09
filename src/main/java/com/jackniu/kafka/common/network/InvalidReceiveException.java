package com.jackniu.kafka.common.network;

import com.jackniu.kafka.common.KafkaException;

public class InvalidReceiveException  extends KafkaException {

    public InvalidReceiveException(String message) {
        super(message);
    }

    public InvalidReceiveException(String message, Throwable cause) {
        super(message, cause);
    }
}

