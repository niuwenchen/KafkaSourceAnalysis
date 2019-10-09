package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.message.ElectPreferredLeadersResponseData;
import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class ElectPreferredLeadersResponse extends AbstractResponse {

    private final ElectPreferredLeadersResponseData data;

    public ElectPreferredLeadersResponse(ElectPreferredLeadersResponseData data) {
        this.data = data;
    }

    public ElectPreferredLeadersResponse(Struct struct, short version) {
        this.data = new ElectPreferredLeadersResponseData(struct, version);
    }

    public ElectPreferredLeadersResponse(Struct struct) {
        short latestVersion = (short) (ElectPreferredLeadersResponseData.SCHEMAS.length - 1);
        this.data = new ElectPreferredLeadersResponseData(struct, latestVersion);
    }

    public ElectPreferredLeadersResponseData data() {
        return data;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        HashMap<Errors, Integer> counts = new HashMap<>();
        for (ElectPreferredLeadersResponseData.ReplicaElectionResult result : data.replicaElectionResults()) {
            for (ElectPreferredLeadersResponseData.PartitionResult partitionResult : result.partitionResult()) {
                Errors error = Errors.forCode(partitionResult.errorCode());
                counts.put(error, counts.getOrDefault(error, 0) + 1);
            }
        }
        return counts;
    }

    public static ElectPreferredLeadersResponse parse(ByteBuffer buffer, short version) {
        return new ElectPreferredLeadersResponse(
                ApiKeys.ELECT_PREFERRED_LEADERS.responseSchema(version).read(buffer), version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 3;
    }
}