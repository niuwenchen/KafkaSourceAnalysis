package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static com.jackniu.kafka.common.protocol.CommonFields.*;

public class HeartbeatRequest extends AbstractRequest {
    private static final Schema HEARTBEAT_REQUEST_V0 = new Schema(
            GROUP_ID,
            GENERATION_ID,
            MEMBER_ID);

    /* v1 request is the same as v0. Throttle time has been added to response */
    private static final Schema HEARTBEAT_REQUEST_V1 = HEARTBEAT_REQUEST_V0;

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema HEARTBEAT_REQUEST_V2 = HEARTBEAT_REQUEST_V1;

    public static Schema[] schemaVersions() {
        return new Schema[] {HEARTBEAT_REQUEST_V0, HEARTBEAT_REQUEST_V1,
                HEARTBEAT_REQUEST_V2};
    }

    public static class Builder extends AbstractRequest.Builder<HeartbeatRequest> {
        private final String groupId;
        private final int groupGenerationId;
        private final String memberId;

        public Builder(String groupId, int groupGenerationId, String memberId) {
            super(ApiKeys.HEARTBEAT);
            this.groupId = groupId;
            this.groupGenerationId = groupGenerationId;
            this.memberId = memberId;
        }

        @Override
        public HeartbeatRequest build(short version) {
            return new HeartbeatRequest(groupId, groupGenerationId, memberId, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=HeartbeatRequest").
                    append(", groupId=").append(groupId).
                    append(", groupGenerationId=").append(groupGenerationId).
                    append(", memberId=").append(memberId).
                    append(")");
            return bld.toString();
        }
    }

    private final String groupId;
    private final int groupGenerationId;
    private final String memberId;

    private HeartbeatRequest(String groupId, int groupGenerationId, String memberId, short version) {
        super(ApiKeys.HEARTBEAT, version);
        this.groupId = groupId;
        this.groupGenerationId = groupGenerationId;
        this.memberId = memberId;
    }

    public HeartbeatRequest(Struct struct, short version) {
        super(ApiKeys.HEARTBEAT, version);
        groupId = struct.get(GROUP_ID);
        groupGenerationId = struct.get(GENERATION_ID);
        memberId = struct.get(MEMBER_ID);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                return new HeartbeatResponse(Errors.forException(e));
            case 1:
            case 2:
                return new HeartbeatResponse(throttleTimeMs, Errors.forException(e));
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.HEARTBEAT.latestVersion()));
        }
    }

    public String groupId() {
        return groupId;
    }

    public int groupGenerationId() {
        return groupGenerationId;
    }

    public String memberId() {
        return memberId;
    }

    public static HeartbeatRequest parse(ByteBuffer buffer, short version) {
        return new HeartbeatRequest(ApiKeys.HEARTBEAT.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.HEARTBEAT.requestSchema(version()));
        struct.set(GROUP_ID, groupId);
        struct.set(GENERATION_ID, groupGenerationId);
        struct.set(MEMBER_ID, memberId);
        return struct;
    }
}

