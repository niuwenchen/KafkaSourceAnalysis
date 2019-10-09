package com.jackniu.kafka.clients.producer.internal;

import com.jackniu.kafka.clients.producer.Callback;
import com.jackniu.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class ErrorLoggingCallback implements Callback {
    private static final Logger log = LoggerFactory.getLogger(ErrorLoggingCallback.class);
    private String topic;
    private byte[] key;
    private byte[] value;
    private int valueLength;
    private boolean logAsString;

    public ErrorLoggingCallback(String topic, byte[] key, byte[] value, boolean logAsString) {
        this.topic = topic;
        this.key = key;

        if (logAsString) {
            this.value = value;
        }

        this.valueLength = value == null ? -1 : value.length;
        this.logAsString = logAsString;
    }

    public void onCompletion(RecordMetadata metadata, Exception e) {
        if (e != null) {
            String keyString = (key == null) ? "null" :
                    logAsString ? new String(key, StandardCharsets.UTF_8) : key.length + " bytes";
            String valueString = (valueLength == -1) ? "null" :
                    logAsString ? new String(value, StandardCharsets.UTF_8) : valueLength + " bytes";
            log.error("Error when sending message to topic {} with key: {}, value: {} with error:",
                    topic, keyString, valueString, e);
        }
    }
}

