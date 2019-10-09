package com.jackniu.kafka.clients;


import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.record.RecordBatch;
import com.jackniu.kafka.common.requests.ProduceRequest;

import java.util.HashMap;
import java.util.Map;

/**
 * Maintains node api versions for access outside of NetworkClient (which is where the information is derived).
 * The pattern is akin to the use of {@link Metadata} for topic metadata.
 *
 * NOTE: This class is intended for INTERNAL usage only within Kafka.
 */
public class ApiVersions {
    private final Map<String, NodeApiVersions> nodeApiVersions = new HashMap<>();
    private byte maxUsableProduceMagic = RecordBatch.CURRENT_MAGIC_VALUE;

    public synchronized void update(String nodeId, NodeApiVersions nodeApiVersions) {
        this.nodeApiVersions.put(nodeId, nodeApiVersions);
        this.maxUsableProduceMagic = computeMaxUsableProduceMagic();
    }

    public synchronized void remove(String nodeId) {
        this.nodeApiVersions.remove(nodeId);
        this.maxUsableProduceMagic = computeMaxUsableProduceMagic();
    }

    public synchronized NodeApiVersions get(String nodeId) {
        return this.nodeApiVersions.get(nodeId);
    }

    private byte computeMaxUsableProduceMagic() {
        // use a magic version which is supported by all brokers to reduce the chance that
        // we will need to convert the messages when they are ready to be sent.
        byte maxUsableMagic = RecordBatch.CURRENT_MAGIC_VALUE;
        for (NodeApiVersions versions : this.nodeApiVersions.values()) {
            byte nodeMaxUsableMagic = ProduceRequest.requiredMagicForVersion(versions.latestUsableVersion(ApiKeys.PRODUCE));
            maxUsableMagic = (byte) Math.min(nodeMaxUsableMagic, maxUsableMagic);
        }
        return maxUsableMagic;
    }

    public synchronized byte maxUsableProduceMagic() {
        return maxUsableProduceMagic;
    }
}
