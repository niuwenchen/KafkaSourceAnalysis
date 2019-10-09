package com.jackniu.kafka.common;

import com.jackniu.kafka.common.config.ConfigException;

import java.util.Map;
import java.util.Set;

public interface Reconfigurable  extends Configurable {

    /**
     * Returns the names of configs that may be reconfigured.
     */
    Set<String> reconfigurableConfigs();

    /**
     * Validates the provided configuration. The provided map contains
     * all configs including any reconfigurable configs that may be different
     * from the initial configuration. Reconfiguration will be not performed
     * if this method throws any exception.
     * @throws ConfigException if the provided configs are not valid. The exception
     *         message from ConfigException will be returned to the client in
     *         the AlterConfigs response.
     */
    void validateReconfiguration(Map<String, ?> configs) throws ConfigException;

    /**
     * Reconfigures this instance with the given key-value pairs. The provided
     * map contains all configs including any reconfigurable configs that
     * may have changed since the object was initially configured using
     * {@link Configurable#configure(Map)}. This method will only be invoked if
     * the configs have passed validation using {@link #validateReconfiguration(Map)}.
     */
    void reconfigure(Map<String, ?> configs);

}

