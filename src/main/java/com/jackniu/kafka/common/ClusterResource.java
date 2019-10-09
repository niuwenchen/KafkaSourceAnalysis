package com.jackniu.kafka.common;

import java.util.Objects;

/**
 * The <code>ClusterResource</code> class encapsulates metadata for a Kafka cluster.
 */
public class ClusterResource {
    private final String clusterId;

    /**
     * Create {@link ClusterResource} with a cluster id. Note that cluster id may be {@code null} if the
     * metadata request was sent to a broker without support for cluster ids. The first version of Kafka
     * to support cluster id is 0.10.1.0.
     * @param clusterId
     */
    public ClusterResource(String clusterId) {
        this.clusterId = clusterId;
    }

    /**
     * Return the cluster id. Note that it may be {@code null} if the metadata request was sent to a broker without
     * support for cluster ids. The first version of Kafka to support cluster id is 0.10.1.0.
     */
    public String clusterId() {
        return clusterId;
    }

    @Override
    public String toString() {
        return "ClusterResource(clusterId=" + clusterId + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterResource that = (ClusterResource) o;
        return Objects.equals(clusterId, that.clusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId);
    }
}
