package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static com.jackniu.kafka.common.protocol.types.Type.BYTES;
import static com.jackniu.kafka.common.protocol.types.Type.INT64;

public class ExpireDelegationTokenRequest  extends AbstractRequest {

    private static final String HMAC_KEY_NAME = "hmac";
    private static final String EXPIRY_TIME_PERIOD_KEY_NAME = "expiry_time_period";
    private final ByteBuffer hmac;
    private final long expiryTimePeriod;

    private static final Schema TOKEN_EXPIRE_REQUEST_V0 = new Schema(
            new Field(HMAC_KEY_NAME, BYTES, "HMAC of the delegation token to be expired."),
            new Field(EXPIRY_TIME_PERIOD_KEY_NAME, INT64, "expiry time period in milli seconds."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema TOKEN_EXPIRE_REQUEST_V1 = TOKEN_EXPIRE_REQUEST_V0;

    private ExpireDelegationTokenRequest(short version, ByteBuffer hmac, long renewTimePeriod) {
        super(ApiKeys.EXPIRE_DELEGATION_TOKEN, version);

        this.hmac = hmac;
        this.expiryTimePeriod = renewTimePeriod;
    }

    public ExpireDelegationTokenRequest(Struct struct, short versionId) {
        super(ApiKeys.EXPIRE_DELEGATION_TOKEN, versionId);

        hmac = struct.getBytes(HMAC_KEY_NAME);
        expiryTimePeriod = struct.getLong(EXPIRY_TIME_PERIOD_KEY_NAME);
    }

    public static ExpireDelegationTokenRequest parse(ByteBuffer buffer, short version) {
        return new ExpireDelegationTokenRequest(ApiKeys.EXPIRE_DELEGATION_TOKEN.parseRequest(version, buffer), version);
    }

    public static Schema[] schemaVersions() {
        return new Schema[] {TOKEN_EXPIRE_REQUEST_V0, TOKEN_EXPIRE_REQUEST_V1};
    }

    @Override
    protected Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.EXPIRE_DELEGATION_TOKEN.requestSchema(version));

        struct.set(HMAC_KEY_NAME, hmac);
        struct.set(EXPIRY_TIME_PERIOD_KEY_NAME, expiryTimePeriod);

        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new ExpireDelegationTokenResponse(throttleTimeMs, Errors.forException(e));
    }

    public ByteBuffer hmac() {
        return hmac;
    }

    public long expiryTimePeriod() {
        return expiryTimePeriod;
    }

    public static class Builder extends AbstractRequest.Builder<ExpireDelegationTokenRequest> {
        private final ByteBuffer hmac;
        private final long expiryTimePeriod;

        public Builder(byte[] hmac, long expiryTimePeriod) {
            super(ApiKeys.EXPIRE_DELEGATION_TOKEN);
            this.hmac = ByteBuffer.wrap(hmac);
            this.expiryTimePeriod = expiryTimePeriod;
        }

        @Override
        public ExpireDelegationTokenRequest build(short version) {
            return new ExpireDelegationTokenRequest(version, hmac, expiryTimePeriod);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type: ExpireDelegationTokenRequest").
                    append(", hmac=").append(hmac).
                    append(", expiryTimePeriod=").append(expiryTimePeriod).
                    append(")");
            return bld.toString();
        }
    }
}

