package com.jackniu.kafka.clients.producer;

import com.jackniu.kafka.common.KafkaException;

public class BufferExhaustedException   extends KafkaException {

    private static final long serialVersionUID = 1L;

    public BufferExhaustedException(String message) {
        super(message);
    }

}

