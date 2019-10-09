package com.jackniu.kafka.common;

import java.util.Map;

public interface Configurable {
    /**
     * Configure this class with the given key-value pairs
     */
    void configure(Map<String, ?> configs);

}
