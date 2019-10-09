package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.jackniu.kafka.common.protocol.CommonFields.ERROR_CODE;

public class SaslHandshakeResponse extends AbstractResponse {
    private static final String ENABLED_MECHANISMS_KEY_NAME = "enabled_mechanisms";

    private static final Schema SASL_HANDSHAKE_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            new Field(ENABLED_MECHANISMS_KEY_NAME, new ArrayOf(Type.STRING), "Array of mechanisms enabled in the server."));

    private static final Schema SASL_HANDSHAKE_RESPONSE_V1 = SASL_HANDSHAKE_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{SASL_HANDSHAKE_RESPONSE_V0, SASL_HANDSHAKE_RESPONSE_V1};
    }

    /**
     * Possible error codes:
     *   UNSUPPORTED_SASL_MECHANISM(33): Client mechanism not enabled in server
     *   ILLEGAL_SASL_STATE(34) : Invalid request during SASL handshake
     */
    private final Errors error;
    private final List<String> enabledMechanisms;

    public SaslHandshakeResponse(Errors error, Collection<String> enabledMechanisms) {
        this.error = error;
        this.enabledMechanisms = new ArrayList<>(enabledMechanisms);
    }

    public SaslHandshakeResponse(Struct struct) {
        error = Errors.forCode(struct.get(ERROR_CODE));
        Object[] mechanisms = struct.getArray(ENABLED_MECHANISMS_KEY_NAME);
        ArrayList<String> enabledMechanisms = new ArrayList<>();
        for (Object mechanism : mechanisms)
            enabledMechanisms.add((String) mechanism);
        this.enabledMechanisms = enabledMechanisms;
    }

    public Errors error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    @Override
    public Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.SASL_HANDSHAKE.responseSchema(version));
        struct.set(ERROR_CODE, error.code());
        struct.set(ENABLED_MECHANISMS_KEY_NAME, enabledMechanisms.toArray());
        return struct;
    }

    public List<String> enabledMechanisms() {
        return enabledMechanisms;
    }

    public static SaslHandshakeResponse parse(ByteBuffer buffer, short version) {
        return new SaslHandshakeResponse(ApiKeys.SASL_HANDSHAKE.parseResponse(version, buffer));
    }

}
