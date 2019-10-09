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
import static com.jackniu.kafka.common.protocol.types.Type.INT64;

public class RenewDelegationTokenResponse  extends AbstractResponse {

    private static final String EXPIRY_TIMESTAMP_KEY_NAME = "expiry_timestamp";

    private final Errors error;
    private final long expiryTimestamp;
    private final int throttleTimeMs;

    private static final Schema TOKEN_RENEW_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            new Field(EXPIRY_TIMESTAMP_KEY_NAME, INT64, "timestamp (in msec) at which this token expires.."),
            THROTTLE_TIME_MS);

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema TOKEN_RENEW_RESPONSE_V1 = TOKEN_RENEW_RESPONSE_V0;

    public RenewDelegationTokenResponse(int throttleTimeMs, Errors error, long expiryTimestamp) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.expiryTimestamp = expiryTimestamp;
    }

    public RenewDelegationTokenResponse(int throttleTimeMs, Errors error) {
        this(throttleTimeMs, error, -1);
    }

    public RenewDelegationTokenResponse(Struct struct) {
        error = Errors.forCode(struct.get(ERROR_CODE));
        expiryTimestamp = struct.getLong(EXPIRY_TIMESTAMP_KEY_NAME);
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
    }

    public static RenewDelegationTokenResponse parse(ByteBuffer buffer, short version) {
        return new RenewDelegationTokenResponse(ApiKeys.RENEW_DELEGATION_TOKEN.responseSchema(version).read(buffer));
    }

    public static Schema[] schemaVersions() {
        return new Schema[] {TOKEN_RENEW_RESPONSE_V0, TOKEN_RENEW_RESPONSE_V1};
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.RENEW_DELEGATION_TOKEN.responseSchema(version));

        struct.set(ERROR_CODE, error.code());
        struct.set(EXPIRY_TIMESTAMP_KEY_NAME, expiryTimestamp);
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);

        return struct;
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Errors error() {
        return error;
    }

    public long expiryTimestamp() {
        return expiryTimestamp;
    }

    public boolean hasError() {
        return this.error != Errors.NONE;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}

