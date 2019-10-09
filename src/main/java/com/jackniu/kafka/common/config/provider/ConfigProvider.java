package com.jackniu.kafka.common.config.provider;

import com.jackniu.kafka.common.Configurable;
import com.jackniu.kafka.common.config.ConfigChangeCallback;
import com.jackniu.kafka.common.config.ConfigData;

import java.io.Closeable;
import java.util.Set;

public interface ConfigProvider extends Configurable, Closeable {

    /**
     * Retrieves the data at the given path.
     *
     * @param path the path where the data resides
     * @return the configuration data
     */
    ConfigData get(String path);

    /**
     * Retrieves the data with the given keys at the given path.
     *
     * @param path the path where the data resides
     * @param keys the keys whose values will be retrieved
     * @return the configuration data
     */
    ConfigData get(String path, Set<String> keys);

    /**
     * Subscribes to changes for the given keys at the given path (optional operation).
     *
     * @param path the path where the data resides
     * @param keys the keys whose values will be retrieved
     * @param callback the callback to invoke upon change
     * @throws {@link UnsupportedOperationException} if the subscribe operation is not supported
     */
    default void subscribe(String path, Set<String> keys, ConfigChangeCallback callback) {
        throw new UnsupportedOperationException();
    }

    /**
     * Unsubscribes to changes for the given keys at the given path (optional operation).
     *
     * @param path the path where the data resides
     * @param keys the keys whose values will be retrieved
     * @param callback the callback to be unsubscribed from changes
     * @throws {@link UnsupportedOperationException} if the unsubscribe operation is not supported
     */
    default void unsubscribe(String path, Set<String> keys, ConfigChangeCallback callback) {
        throw new UnsupportedOperationException();
    }

    /**
     * Clears all subscribers (optional operation).
     *
     * @throws {@link UnsupportedOperationException} if the unsubscribeAll operation is not supported
     */
    default void unsubscribeAll() {
        throw new UnsupportedOperationException();
    }
}
