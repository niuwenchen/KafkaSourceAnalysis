package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.jackniu.kafka.common.protocol.CommonFields.ERROR_CODE;
import static com.jackniu.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static com.jackniu.kafka.common.protocol.types.Type.BYTES;

public class SyncGroupResponse extends AbstractResponse {
    private static final String MEMBER_ASSIGNMENT_KEY_NAME = "member_assignment";

    private static final Schema SYNC_GROUP_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            new Field(MEMBER_ASSIGNMENT_KEY_NAME, BYTES));
    private static final Schema SYNC_GROUP_RESPONSE_V1 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE,
            new Field(MEMBER_ASSIGNMENT_KEY_NAME, BYTES));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema SYNC_GROUP_RESPONSE_V2 = SYNC_GROUP_RESPONSE_V1;

    public static Schema[] schemaVersions() {
        return new Schema[] {SYNC_GROUP_RESPONSE_V0, SYNC_GROUP_RESPONSE_V1,
                SYNC_GROUP_RESPONSE_V2};
    }

    /**
     * Possible error codes:
     *
     * COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR (16)
     * ILLEGAL_GENERATION (22)
     * UNKNOWN_MEMBER_ID (25)
     * REBALANCE_IN_PROGRESS (27)
     * GROUP_AUTHORIZATION_FAILED (30)
     *
     * NOTE: Currently the coordinator returns REBALANCE_IN_PROGRESS while the coordinator is
     * loading. On the next protocol bump, we should consider using COORDINATOR_LOAD_IN_PROGRESS
     * to be consistent with the other APIs.
     */

    private final Errors error;
    private final int throttleTimeMs;
    private final ByteBuffer memberState;

    public SyncGroupResponse(Errors error, ByteBuffer memberState) {
        this(DEFAULT_THROTTLE_TIME, error, memberState);
    }

    public SyncGroupResponse(int throttleTimeMs, Errors error, ByteBuffer memberState) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.memberState = memberState;
    }

    public SyncGroupResponse(Struct struct) {
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        this.error = Errors.forCode(struct.get(ERROR_CODE));
        this.memberState = struct.getBytes(MEMBER_ASSIGNMENT_KEY_NAME);
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Errors error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    public ByteBuffer memberAssignment() {
        return memberState;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.SYNC_GROUP.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        struct.set(ERROR_CODE, error.code());
        struct.set(MEMBER_ASSIGNMENT_KEY_NAME, memberState);
        return struct;
    }

    public static SyncGroupResponse parse(ByteBuffer buffer, short version) {
        return new SyncGroupResponse(ApiKeys.SYNC_GROUP.parseResponse(version, buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }
}
