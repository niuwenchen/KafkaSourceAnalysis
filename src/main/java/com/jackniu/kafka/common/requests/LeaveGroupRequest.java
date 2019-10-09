package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class LeaveGroupRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<LeaveGroupRequest> {

        private final LeaveGroupRequestData data;

        public Builder(LeaveGroupRequestData data) {
            super(ApiKeys.LEAVE_GROUP);
            this.data = data;
        }

        @Override
        public LeaveGroupRequest build(short version) {
            return new LeaveGroupRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
    private final LeaveGroupRequestData data;
    private final short version;

    private LeaveGroupRequest(LeaveGroupRequestData data, short version) {
        super(ApiKeys.LEAVE_GROUP, version);
        this.data = data;
        this.version = version;
    }

    public LeaveGroupRequest(Struct struct, short version) {
        super(ApiKeys.LEAVE_GROUP, version);
        this.data = new LeaveGroupRequestData(struct, version);
        this.version = version;
    }

    public LeaveGroupRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LeaveGroupResponseData response = new LeaveGroupResponseData();
        if (version() >= 2) {
            response.setThrottleTimeMs(throttleTimeMs);
        }
        return new LeaveGroupResponse(response);
    }

    public static LeaveGroupRequest parse(ByteBuffer buffer, short version) {
        return new LeaveGroupRequest(ApiKeys.LEAVE_GROUP.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }
}

