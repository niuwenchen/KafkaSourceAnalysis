package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.jackniu.kafka.common.protocol.CommonFields.ERROR_CODE;
import static com.jackniu.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;

public class HeartbeatResponse extends AbstractResponse {

    private static final Schema HEARTBEAT_RESPONSE_V0 = new Schema(
            ERROR_CODE);
    private static final Schema HEARTBEAT_RESPONSE_V1 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE);

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema HEARTBEAT_RESPONSE_V2 = HEARTBEAT_RESPONSE_V1;

    public static Schema[] schemaVersions() {
        return new Schema[] {HEARTBEAT_RESPONSE_V0, HEARTBEAT_RESPONSE_V1,
                HEARTBEAT_RESPONSE_V2};
    }

    /**
     * Possible error codes:
     *
     * GROUP_COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR (16)
     * ILLEGAL_GENERATION (22)
     * UNKNOWN_MEMBER_ID (25)
     * REBALANCE_IN_PROGRESS (27)
     * GROUP_AUTHORIZATION_FAILED (30)
     */
    private final Errors error;
    private final int throttleTimeMs;

    public HeartbeatResponse(Errors error) {
        this(DEFAULT_THROTTLE_TIME, error);
    }

    public HeartbeatResponse(int throttleTimeMs, Errors error) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
    }

    public HeartbeatResponse(Struct struct) {
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        error = Errors.forCode(struct.get(ERROR_CODE));
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

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.HEARTBEAT.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        struct.set(ERROR_CODE, error.code());
        return struct;
    }

    public static HeartbeatResponse parse(ByteBuffer buffer, short version) {
        return new HeartbeatResponse(ApiKeys.HEARTBEAT.parseResponse(version, buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }
}
