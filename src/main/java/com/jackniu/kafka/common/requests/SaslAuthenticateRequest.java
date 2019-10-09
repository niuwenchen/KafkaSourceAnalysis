package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static com.jackniu.kafka.common.protocol.types.Type.BYTES;

public class SaslAuthenticateRequest   extends AbstractRequest {
    private static final String SASL_AUTH_BYTES_KEY_NAME = "sasl_auth_bytes";

    private static final Schema SASL_AUTHENTICATE_REQUEST_V0 = new Schema(
            new Field(SASL_AUTH_BYTES_KEY_NAME, BYTES, "SASL authentication bytes from client as defined by the SASL mechanism."));

    /* v1 request is the same as v0; session_lifetime_ms has been added to the response */
    private static final Schema SASL_AUTHENTICATE_REQUEST_V1 = SASL_AUTHENTICATE_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{SASL_AUTHENTICATE_REQUEST_V0, SASL_AUTHENTICATE_REQUEST_V1};
    }

    private final ByteBuffer saslAuthBytes;

    public static class Builder extends AbstractRequest.Builder<SaslAuthenticateRequest> {
        private final ByteBuffer saslAuthBytes;

        public Builder(ByteBuffer saslAuthBytes) {
            super(ApiKeys.SASL_AUTHENTICATE);
            this.saslAuthBytes = saslAuthBytes;
        }

        @Override
        public SaslAuthenticateRequest build(short version) {
            return new SaslAuthenticateRequest(saslAuthBytes, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=SaslAuthenticateRequest)");
            return bld.toString();
        }
    }

    public SaslAuthenticateRequest(ByteBuffer saslAuthBytes) {
        this(saslAuthBytes, ApiKeys.SASL_AUTHENTICATE.latestVersion());
    }

    public SaslAuthenticateRequest(ByteBuffer saslAuthBytes, short version) {
        super(ApiKeys.SASL_AUTHENTICATE, version);
        this.saslAuthBytes = saslAuthBytes;
    }

    public SaslAuthenticateRequest(Struct struct, short version) {
        super(ApiKeys.SASL_AUTHENTICATE, version);
        saslAuthBytes = struct.getBytes(SASL_AUTH_BYTES_KEY_NAME);
    }

    public ByteBuffer saslAuthBytes() {
        return saslAuthBytes;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new SaslAuthenticateResponse(Errors.forException(e), e.getMessage());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.SASL_AUTHENTICATE.latestVersion()));
        }
    }

    public static SaslAuthenticateRequest parse(ByteBuffer buffer, short version) {
        return new SaslAuthenticateRequest(ApiKeys.SASL_AUTHENTICATE.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.SASL_AUTHENTICATE.requestSchema(version()));
        struct.set(SASL_AUTH_BYTES_KEY_NAME, saslAuthBytes);
        return struct;
    }
}


