package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.security.auth.KafkaPrincipal;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.jackniu.kafka.common.protocol.CommonFields.*;
import static com.jackniu.kafka.common.protocol.types.Type.*;

public class CreateDelegationTokenResponse extends AbstractResponse {

    private static final String OWNER_KEY_NAME = "owner";
    private static final String ISSUE_TIMESTAMP_KEY_NAME = "issue_timestamp";
    private static final String EXPIRY_TIMESTAMP_NAME = "expiry_timestamp";
    private static final String MAX_TIMESTAMP_NAME = "max_timestamp";
    private static final String TOKEN_ID_KEY_NAME = "token_id";
    private static final String HMAC_KEY_NAME = "hmac";

    private final Errors error;
    private final long issueTimestamp;
    private final long expiryTimestamp;
    private final long maxTimestamp;
    private final String tokenId;
    private final ByteBuffer hmac;
    private final int throttleTimeMs;
    private KafkaPrincipal owner;

    private static final Schema TOKEN_CREATE_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            new Field(OWNER_KEY_NAME, new Schema(PRINCIPAL_TYPE, PRINCIPAL_NAME), "token owner."),
            new Field(ISSUE_TIMESTAMP_KEY_NAME, INT64, "timestamp (in msec) when this token was generated."),
            new Field(EXPIRY_TIMESTAMP_NAME, INT64, "timestamp (in msec) at which this token expires."),
            new Field(MAX_TIMESTAMP_NAME, INT64, "max life time of this token."),
            new Field(TOKEN_ID_KEY_NAME, STRING, "UUID to ensure uniqueness."),
            new Field(HMAC_KEY_NAME, BYTES, "HMAC of the delegation token."),
            THROTTLE_TIME_MS);

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema TOKEN_CREATE_RESPONSE_V1 = TOKEN_CREATE_RESPONSE_V0;

    public CreateDelegationTokenResponse(int throttleTimeMs,
                                         Errors error,
                                         KafkaPrincipal owner,
                                         long issueTimestamp,
                                         long expiryTimestamp,
                                         long maxTimestamp,
                                         String tokenId,
                                         ByteBuffer hmac) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.owner = owner;
        this.issueTimestamp = issueTimestamp;
        this.expiryTimestamp = expiryTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.tokenId = tokenId;
        this.hmac = hmac;
    }

    public CreateDelegationTokenResponse(int throttleTimeMs, Errors error, KafkaPrincipal owner) {
        this(throttleTimeMs, error, owner, -1, -1, -1, "", ByteBuffer.wrap(new byte[] {}));
    }

    public CreateDelegationTokenResponse(Struct struct) {
        error = Errors.forCode(struct.get(ERROR_CODE));
        Struct ownerStruct = (Struct) struct.get(OWNER_KEY_NAME);
        String principalType = ownerStruct.get(PRINCIPAL_TYPE);
        String principalName = ownerStruct.get(PRINCIPAL_NAME);
        owner = new KafkaPrincipal(principalType, principalName);
        issueTimestamp = struct.getLong(ISSUE_TIMESTAMP_KEY_NAME);
        expiryTimestamp = struct.getLong(EXPIRY_TIMESTAMP_NAME);
        maxTimestamp = struct.getLong(MAX_TIMESTAMP_NAME);
        tokenId = struct.getString(TOKEN_ID_KEY_NAME);
        hmac = struct.getBytes(HMAC_KEY_NAME);
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
    }

    public static CreateDelegationTokenResponse parse(ByteBuffer buffer, short version) {
        return new CreateDelegationTokenResponse(ApiKeys.CREATE_DELEGATION_TOKEN.responseSchema(version).read(buffer));
    }

    public static Schema[] schemaVersions() {
        return new Schema[] {TOKEN_CREATE_RESPONSE_V0, TOKEN_CREATE_RESPONSE_V1};
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.CREATE_DELEGATION_TOKEN.responseSchema(version));
        struct.set(ERROR_CODE, error.code());
        Struct ownerStruct = struct.instance(OWNER_KEY_NAME);
        ownerStruct.set(PRINCIPAL_TYPE, owner.getPrincipalType());
        ownerStruct.set(PRINCIPAL_NAME, owner.getName());
        struct.set(OWNER_KEY_NAME, ownerStruct);
        struct.set(ISSUE_TIMESTAMP_KEY_NAME, issueTimestamp);
        struct.set(EXPIRY_TIMESTAMP_NAME, expiryTimestamp);
        struct.set(MAX_TIMESTAMP_NAME, maxTimestamp);
        struct.set(TOKEN_ID_KEY_NAME, tokenId);
        struct.set(HMAC_KEY_NAME, hmac);
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        return struct;
    }

    public Errors error() {
        return error;
    }

    public KafkaPrincipal owner() {
        return owner;
    }

    public long issueTimestamp() {
        return issueTimestamp;
    }

    public long expiryTimestamp() {
        return expiryTimestamp;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public String tokenId() {
        return tokenId;
    }

    public byte[] hmacBytes() {
        byte[] byteArray = new byte[hmac.remaining()];
        hmac.get(byteArray);
        return byteArray;
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public boolean hasError() {
        return this.error != Errors.NONE;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
