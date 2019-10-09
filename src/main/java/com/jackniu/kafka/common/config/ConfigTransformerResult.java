package com.jackniu.kafka.common.config;

import java.util.Map;

public class ConfigTransformerResult {
    private Map<String, Long> ttls;
    private Map<String, String> data;

    /**
     * Creates a new ConfigTransformerResult with the given data and TTL values for a set of paths.
     *
     * @param data a Map of key-value pairs
     * @param ttls a Map of path and TTL values (in milliseconds)
     */
    public ConfigTransformerResult(Map<String, String> data, Map<String, Long> ttls) {
        this.data = data;
        this.ttls = ttls;
    }

    /**
     * Returns the transformed data, with variables replaced with corresponding values from the
     * ConfigProvider instances if found.
     *
     * <p>Modifying the transformed data that is returned does not affect the {@link } nor the
     * original data that was used as the source of the transformation.
     *
     * @return data a Map of key-value pairs
     */
    public Map<String, String> data() {
        return data;
    }

    /**
     * Returns the TTL values (in milliseconds) returned from the ConfigProvider instances for a given set of paths.
     *
     * @return data a Map of path and TTL values
     */
    public Map<String, Long> ttls() {
        return ttls;
    }
}
