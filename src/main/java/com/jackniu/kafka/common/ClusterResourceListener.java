package com.jackniu.kafka.common;

public interface ClusterResourceListener {
    /**
     * A callback method that a user can implement to get updates for {@link ClusterResource}.
     * @param clusterResource cluster metadata
     */
    void onUpdate(ClusterResource clusterResource);
}
