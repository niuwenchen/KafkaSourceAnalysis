package com.jackniu.kafka.common.internals;

import com.jackniu.kafka.common.ClusterResource;
import com.jackniu.kafka.common.ClusterResourceListener;

import java.util.ArrayList;
import java.util.List;

public class ClusterResourceListeners {
    private final List<ClusterResourceListener> clusterResourceListeners;

    public ClusterResourceListeners() {
        this.clusterResourceListeners = new ArrayList<>();
    }

    /**
     * Add only if the candidate implements {@link ClusterResourceListener}.
     * @param candidate Object which might implement {@link ClusterResourceListener}
     */
    public void maybeAdd(Object candidate) {
        if (candidate instanceof ClusterResourceListener) {
            clusterResourceListeners.add((ClusterResourceListener) candidate);
        }
    }

    /**
     * Add all items who implement {@link ClusterResourceListener} from the list.
     * @param candidateList List of objects which might implement {@link ClusterResourceListener}
     */
    public void maybeAddAll(List<?> candidateList) {
        for (Object candidate : candidateList) {
            this.maybeAdd(candidate);
        }
    }

    /**
     * Send the updated cluster metadata to all {@link ClusterResourceListener}.
     * @param cluster Cluster metadata
     */
    public void onUpdate(ClusterResource cluster) {
        for (ClusterResourceListener clusterResourceListener : clusterResourceListeners) {
            clusterResourceListener.onUpdate(cluster);
        }
    }
}
