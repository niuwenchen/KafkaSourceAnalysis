package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.jackniu.kafka.common.protocol.CommonFields.ERROR_CODE;
import static com.jackniu.kafka.common.protocol.CommonFields.ERROR_MESSAGE;
import static com.jackniu.kafka.common.protocol.types.Type.BYTES;
import static com.jackniu.kafka.common.protocol.types.Type.INT64;

public class SaslAuthenticateResponse extends AbstractResponse {
    private static final String SASL_AUTH_BYTES_KEY_NAME = "sasl_auth_bytes";
    private static final String SESSION_LIFETIME_MS = "session_lifetime_ms";

    private static final Schema SASL_AUTHENTICATE_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            ERROR_MESSAGE,
            new Field(SASL_AUTH_BYTES_KEY_NAME, BYTES, "SASL authentication bytes from server as defined by the SASL mechanism."));

    private static final Schema SASL_AUTHENTICATE_RESPONSE_V1 = new Schema(
            ERROR_CODE,
            ERROR_MESSAGE,
            new Field(SASL_AUTH_BYTES_KEY_NAME, BYTES, "SASL authentication bytes from server as defined by the SASL mechanism."),
            new Field(SESSION_LIFETIME_MS, INT64, "Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur."));

    public static Schema[] schemaVersions() {
        return new Schema[]{SASL_AUTHENTICATE_RESPONSE_V0, SASL_AUTHENTICATE_RESPONSE_V1};
    }

    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private final ByteBuffer saslAuthBytes;

    /**
     * Possible error codes:
     *   SASL_AUTHENTICATION_FAILED(57) : Authentication failed
     */
    private final Errors error;
    private final String errorMessage;
    private final long sessionLifetimeMs;

    public SaslAuthenticateResponse(Errors error, String errorMessage) {
        this(error, errorMessage, EMPTY_BUFFER);
    }

    public SaslAuthenticateResponse(Errors error, String errorMessage, ByteBuffer saslAuthBytes) {
        this(error, errorMessage, saslAuthBytes, 0L);
    }

    public SaslAuthenticateResponse(Errors error, String errorMessage, ByteBuffer saslAuthBytes, long sessionLifetimeMs) {
        this.error = error;
        this.errorMessage = errorMessage;
        this.saslAuthBytes = saslAuthBytes;
        this.sessionLifetimeMs = sessionLifetimeMs;
    }

    public SaslAuthenticateResponse(Struct struct) {
        error = Errors.forCode(struct.get(ERROR_CODE));
        errorMessage = struct.get(ERROR_MESSAGE);
        saslAuthBytes = struct.getBytes(SASL_AUTH_BYTES_KEY_NAME);
        sessionLifetimeMs = struct.hasField(SESSION_LIFETIME_MS) ? struct.getLong(SESSION_LIFETIME_MS).longValue() : 0L;
    }

    public Errors error() {
        return error;
    }

    public String errorMessage() {
        return errorMessage;
    }

    public ByteBuffer saslAuthBytes() {
        return saslAuthBytes;
    }

    public long sessionLifetimeMs() {
        return sessionLifetimeMs;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    @Override
    public Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.SASL_AUTHENTICATE.responseSchema(version));
        struct.set(ERROR_CODE, error.code());
        struct.set(ERROR_MESSAGE, errorMessage);
        struct.set(SASL_AUTH_BYTES_KEY_NAME, saslAuthBytes);
        if (version > 0)
            struct.set(SESSION_LIFETIME_MS, sessionLifetimeMs);
        return struct;
    }

    public static SaslAuthenticateResponse parse(ByteBuffer buffer, short version) {
        return new SaslAuthenticateResponse(ApiKeys.SASL_AUTHENTICATE.parseResponse(version, buffer));
    }
}
