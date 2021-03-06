package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.ArrayOf;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.jackniu.kafka.common.protocol.CommonFields.*;
import static com.jackniu.kafka.common.protocol.types.Type.BYTES;

public class SyncGroupRequest  extends AbstractRequest {
    private static final String MEMBER_ASSIGNMENT_KEY_NAME = "member_assignment";
    private static final String GROUP_ASSIGNMENT_KEY_NAME = "group_assignment";

    private static final Schema SYNC_GROUP_REQUEST_MEMBER_V0 = new Schema(
            MEMBER_ID,
            new Field(MEMBER_ASSIGNMENT_KEY_NAME, BYTES));
    private static final Schema SYNC_GROUP_REQUEST_V0 = new Schema(
            GROUP_ID,
            GENERATION_ID,
            MEMBER_ID,
            new Field(GROUP_ASSIGNMENT_KEY_NAME, new ArrayOf(SYNC_GROUP_REQUEST_MEMBER_V0)));

    /* v1 request is the same as v0. Throttle time has been added to response */
    private static final Schema SYNC_GROUP_REQUEST_V1 = SYNC_GROUP_REQUEST_V0;

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema SYNC_GROUP_REQUEST_V2 = SYNC_GROUP_REQUEST_V1;

    public static Schema[] schemaVersions() {
        return new Schema[] {SYNC_GROUP_REQUEST_V0, SYNC_GROUP_REQUEST_V1,
                SYNC_GROUP_REQUEST_V2};
    }

    public static class Builder extends AbstractRequest.Builder<SyncGroupRequest> {
        private final String groupId;
        private final int generationId;
        private final String memberId;
        private final Map<String, ByteBuffer> groupAssignment;

        public Builder(String groupId, int generationId, String memberId,
                       Map<String, ByteBuffer> groupAssignment) {
            super(ApiKeys.SYNC_GROUP);
            this.groupId = groupId;
            this.generationId = generationId;
            this.memberId = memberId;
            this.groupAssignment = groupAssignment;
        }

        @Override
        public SyncGroupRequest build(short version) {
            return new SyncGroupRequest(groupId, generationId, memberId, groupAssignment, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=SyncGroupRequest").
                    append(", groupId=").append(groupId).
                    append(", generationId=").append(generationId).
                    append(", memberId=").append(memberId).
                    append(", groupAssignment=").
                    append(Utils.join(groupAssignment.keySet(), ",")).
                    append(")");
            return bld.toString();
        }
    }
    private final String groupId;
    private final int generationId;
    private final String memberId;
    private final Map<String, ByteBuffer> groupAssignment;

    private SyncGroupRequest(String groupId, int generationId, String memberId,
                             Map<String, ByteBuffer> groupAssignment, short version) {
        super(ApiKeys.SYNC_GROUP, version);
        this.groupId = groupId;
        this.generationId = generationId;
        this.memberId = memberId;
        this.groupAssignment = groupAssignment;
    }

    public SyncGroupRequest(Struct struct, short version) {
        super(ApiKeys.SYNC_GROUP, version);
        this.groupId = struct.get(GROUP_ID);
        this.generationId = struct.get(GENERATION_ID);
        this.memberId = struct.get(MEMBER_ID);

        groupAssignment = new HashMap<>();

        for (Object memberDataObj : struct.getArray(GROUP_ASSIGNMENT_KEY_NAME)) {
            Struct memberData = (Struct) memberDataObj;
            String memberId = memberData.get(MEMBER_ID);
            ByteBuffer memberMetadata = memberData.getBytes(MEMBER_ASSIGNMENT_KEY_NAME);
            groupAssignment.put(memberId, memberMetadata);
        }
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                return new SyncGroupResponse(
                        Errors.forException(e),
                        ByteBuffer.wrap(new byte[]{}));
            case 1:
            case 2:
                return new SyncGroupResponse(
                        throttleTimeMs,
                        Errors.forException(e),
                        ByteBuffer.wrap(new byte[]{}));
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.SYNC_GROUP.latestVersion()));
        }
    }

    public String groupId() {
        return groupId;
    }

    public int generationId() {
        return generationId;
    }

    public Map<String, ByteBuffer> groupAssignment() {
        return groupAssignment;
    }

    public String memberId() {
        return memberId;
    }

    public static SyncGroupRequest parse(ByteBuffer buffer, short version) {
        return new SyncGroupRequest(ApiKeys.SYNC_GROUP.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.SYNC_GROUP.requestSchema(version()));
        struct.set(GROUP_ID, groupId);
        struct.set(GENERATION_ID, generationId);
        struct.set(MEMBER_ID, memberId);

        List<Struct> memberArray = new ArrayList<>();
        for (Map.Entry<String, ByteBuffer> entries: groupAssignment.entrySet()) {
            Struct memberData = struct.instance(GROUP_ASSIGNMENT_KEY_NAME);
            memberData.set(MEMBER_ID, entries.getKey());
            memberData.set(MEMBER_ASSIGNMENT_KEY_NAME, entries.getValue());
            memberArray.add(memberData);
        }
        struct.set(GROUP_ASSIGNMENT_KEY_NAME, memberArray.toArray());
        return struct;
    }
}

