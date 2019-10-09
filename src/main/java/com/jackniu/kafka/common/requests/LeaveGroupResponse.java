package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class LeaveGroupResponse extends AbstractResponse {

    private final LeaveGroupResponseData data;

    public LeaveGroupResponse(LeaveGroupResponseData data) {
        this.data = data;
    }

    public LeaveGroupResponse(Struct struct) {
        short latestVersion = (short) (LeaveGroupResponseData.SCHEMAS.length - 1);
        this.data = new LeaveGroupResponseData(struct, latestVersion);
    }
    public LeaveGroupResponse(Struct struct, short version) {
        this.data = new LeaveGroupResponseData(struct, version);
    }

    public LeaveGroupResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return Collections.singletonMap(Errors.forCode(data.errorCode()), 1);
    }

    @Override
    public Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public static LeaveGroupResponse parse(ByteBuffer buffer, short versionId) {
        return new LeaveGroupResponse(ApiKeys.LEAVE_GROUP.parseResponse(versionId, buffer), versionId);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }
}

