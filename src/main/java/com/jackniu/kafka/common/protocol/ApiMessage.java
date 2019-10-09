package com.jackniu.kafka.common.protocol;

public interface ApiMessage extends Message {
    /**
     * Returns the API key of this message, or -1 if there is none.
     */
    short apiKey();
}

