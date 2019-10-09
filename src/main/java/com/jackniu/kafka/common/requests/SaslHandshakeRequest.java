package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static com.jackniu.kafka.common.protocol.types.Type.STRING;

public class SaslHandshakeRequest extends AbstractRequest {
    private static final String MECHANISM_KEY_NAME = "mechanism";

    private static final Schema SASL_HANDSHAKE_REQUEST_V0 = new Schema(
            new Field("mechanism", STRING, "SASL Mechanism chosen by the client."));

    // SASL_HANDSHAKE_REQUEST_V1 added to support SASL_AUTHENTICATE request to improve diagnostics
    private static final Schema SASL_HANDSHAKE_REQUEST_V1 = SASL_HANDSHAKE_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{SASL_HANDSHAKE_REQUEST_V0, SASL_HANDSHAKE_REQUEST_V1};
    }

    private final String mechanism;

    public static class Builder extends AbstractRequest.Builder<SaslHandshakeRequest> {
        private final String mechanism;

        public Builder(String mechanism) {
            super(ApiKeys.SASL_HANDSHAKE);
            this.mechanism = mechanism;
        }

        @Override
        public SaslHandshakeRequest build(short version) {
            return new SaslHandshakeRequest(mechanism, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=SaslHandshakeRequest").
                    append(", mechanism=").append(mechanism).
                    append(")");
            return bld.toString();
        }
    }

    public SaslHandshakeRequest(String mechanism) {
        this(mechanism, ApiKeys.SASL_HANDSHAKE.latestVersion());
    }

    public SaslHandshakeRequest(String mechanism, short version) {
        super(ApiKeys.SASL_HANDSHAKE, version);
        this.mechanism = mechanism;
    }

    public SaslHandshakeRequest(Struct struct, short version) {
        super(ApiKeys.SASL_HANDSHAKE, version);
        mechanism = struct.getString(MECHANISM_KEY_NAME);
    }

    public String mechanism() {
        return mechanism;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                List<String> enabledMechanisms = Collections.emptyList();
                return new SaslHandshakeResponse(Errors.forException(e), enabledMechanisms);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.SASL_HANDSHAKE.latestVersion()));
        }
    }

    public static SaslHandshakeRequest parse(ByteBuffer buffer, short version) {
        return new SaslHandshakeRequest(ApiKeys.SASL_HANDSHAKE.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.SASL_HANDSHAKE.requestSchema(version()));
        struct.set(MECHANISM_KEY_NAME, mechanism);
        return struct;
    }
}


