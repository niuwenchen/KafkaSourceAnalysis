package com.jackniu.kafka.common.config;

public interface ConfigChangeCallback  {
    /**
     * Performs an action when configuration data changes.
     *
     * @param path the path at which the data resides
     * @param data the configuration data
     */
    void onChange(String path, ConfigData data);

}
