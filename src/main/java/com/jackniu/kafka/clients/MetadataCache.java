package com.jackniu.kafka.clients;

import com.jackniu.kafka.common.Cluster;
import com.jackniu.kafka.common.Node;
import com.jackniu.kafka.common.PartitionInfo;
import com.jackniu.kafka.common.TopicPartition;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;

public class MetadataCache  {
    private final String clusterId;
    private final List<Node> nodes;
    private final Set<String> unauthorizedTopics;
    private final Set<String> invalidTopics;
    private final Set<String> internalTopics;
    private final Node controller;
    private final Map<TopicPartition, PartitionInfoAndEpoch> metadataByPartition;

    private Cluster clusterInstance;

    MetadataCache(String clusterId,
                  List<Node> nodes,
                  Collection<PartitionInfoAndEpoch> partitions,
                  Set<String> unauthorizedTopics,
                  Set<String> invalidTopics,
                  Set<String> internalTopics,
                  Node controller) {
        this(clusterId, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller, null);
    }

    MetadataCache(String clusterId,
                  List<Node> nodes,
                  Collection<PartitionInfoAndEpoch> partitions,
                  Set<String> unauthorizedTopics,
                  Set<String> invalidTopics,
                  Set<String> internalTopics,
                  Node controller,
                  Cluster clusterInstance) {
        this.clusterId = clusterId;
        this.nodes = nodes;
        this.unauthorizedTopics = unauthorizedTopics;
        this.invalidTopics = invalidTopics;
        this.internalTopics = internalTopics;
        this.controller = controller;

        this.metadataByPartition = new HashMap<>(partitions.size());
        for (PartitionInfoAndEpoch p : partitions) {
            this.metadataByPartition.put(new TopicPartition(p.partitionInfo().topic(), p.partitionInfo().partition()), p);
        }

        if (clusterInstance == null) {
            computeClusterView();
        } else {
            this.clusterInstance = clusterInstance;
        }
    }

    /**
     * Return the cached PartitionInfo iff it was for the given epoch
     */
    Optional<PartitionInfo> getPartitionInfoHavingEpoch(TopicPartition topicPartition, int epoch) {
        PartitionInfoAndEpoch infoAndEpoch = metadataByPartition.get(topicPartition);
        if (infoAndEpoch == null) {
            return Optional.empty();
        } else {
            if (infoAndEpoch.epoch() == epoch) {
                return Optional.of(infoAndEpoch.partitionInfo());
            } else {
                return Optional.empty();
            }
        }
    }

    Optional<PartitionInfo> getPartitionInfo(TopicPartition topicPartition) {
        return Optional.ofNullable(metadataByPartition.get(topicPartition))
                .map(PartitionInfoAndEpoch::partitionInfo);
    }

    synchronized void retainTopics(Collection<String> topics) {
        metadataByPartition.entrySet().removeIf(entry -> !topics.contains(entry.getKey().topic()));
        unauthorizedTopics.retainAll(topics);
        invalidTopics.retainAll(topics);
        computeClusterView();
    }

    Cluster cluster() {
        if (clusterInstance == null) {
            throw new IllegalStateException("Cached Cluster instance should not be null, but was.");
        } else {
            return clusterInstance;
        }
    }

    private void computeClusterView() {
        System.out.println("MetadataCache 类是做什么的");
        List<PartitionInfo> partitionInfos = metadataByPartition.values()
                .stream()
                .map(PartitionInfoAndEpoch::partitionInfo)
                .collect(Collectors.toList());
        this.clusterInstance = new Cluster(clusterId, nodes, partitionInfos, unauthorizedTopics, invalidTopics, internalTopics, controller);
    }

    static MetadataCache bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses)
            nodes.add(new Node(nodeId--, address.getHostString(), address.getPort()));
        return new MetadataCache(null, nodes, Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Cluster.bootstrap(addresses));
    }

    static MetadataCache empty() {
        return new MetadataCache(null, Collections.emptyList(), Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Cluster.empty());
    }

    @Override
    public String toString() {
        return "MetadataCache{" +
                "cluster=" + cluster() +
                '}';
    }

    static class PartitionInfoAndEpoch {
        private final PartitionInfo partitionInfo;
        private final int epoch;

        PartitionInfoAndEpoch(PartitionInfo partitionInfo, int epoch) {
            this.partitionInfo = partitionInfo;
            this.epoch = epoch;
        }

        public PartitionInfo partitionInfo() {
            return partitionInfo;
        }

        public int epoch() {
            return epoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartitionInfoAndEpoch that = (PartitionInfoAndEpoch) o;
            return epoch == that.epoch &&
                    Objects.equals(partitionInfo, that.partitionInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionInfo, epoch);
        }

        @Override
        public String toString() {
            return "PartitionInfoAndEpoch{" +
                    "partitionInfo=" + partitionInfo +
                    ", epoch=" + epoch +
                    '}';
        }
    }
}
