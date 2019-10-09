package com.jackniu.kafka.common.config;

import java.util.Map;

public class ConfigData {
    private final Map<String, String> data;
    private final Long ttl;

    /**
     * Creates a new ConfigData with the given data and TTL (in milliseconds).
     *
     * @param data a Map of key-value pairs
     * @param ttl the time-to-live of the data in milliseconds, or null if there is no TTL
     */
    public ConfigData(Map<String, String> data, Long ttl) {
        this.data = data;
        this.ttl = ttl;
    }

    /**
     * Creates a new ConfigData with the given data.
     *
     * @param data a Map of key-value pairs
     */
    public ConfigData(Map<String, String> data) {
        this(data, null);
    }

    /**
     * Returns the data.
     *
     * @return data a Map of key-value pairs
     */
    public Map<String, String> data() {
        return data;
    }

    /**
     * Returns the TTL (in milliseconds).
     *
     * @return ttl the time-to-live (in milliseconds) of the data, or null if there is no TTL
     */
    public Long ttl() {
        return ttl;
    }
}
