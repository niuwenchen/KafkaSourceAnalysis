package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.jackniu.kafka.common.protocol.CommonFields.ERROR_CODE;

public class UpdateMetadataResponse extends AbstractResponse {
    private static final Schema UPDATE_METADATA_RESPONSE_V0 = new Schema(ERROR_CODE);
    private static final Schema UPDATE_METADATA_RESPONSE_V1 = UPDATE_METADATA_RESPONSE_V0;
    private static final Schema UPDATE_METADATA_RESPONSE_V2 = UPDATE_METADATA_RESPONSE_V1;
    private static final Schema UPDATE_METADATA_RESPONSE_V3 = UPDATE_METADATA_RESPONSE_V2;
    private static final Schema UPDATE_METADATA_RESPONSE_V4 = UPDATE_METADATA_RESPONSE_V3;
    private static final Schema UPDATE_METADATA_RESPONSE_V5 = UPDATE_METADATA_RESPONSE_V4;

    public static Schema[] schemaVersions() {
        return new Schema[]{UPDATE_METADATA_RESPONSE_V0, UPDATE_METADATA_RESPONSE_V1, UPDATE_METADATA_RESPONSE_V2,
                UPDATE_METADATA_RESPONSE_V3, UPDATE_METADATA_RESPONSE_V4, UPDATE_METADATA_RESPONSE_V5};
    }

    /**
     * Possible error code:
     *
     * STALE_CONTROLLER_EPOCH (11)
     * STALE_BROKER_EPOCH (77)
     */
    private final Errors error;

    public UpdateMetadataResponse(Errors error) {
        this.error = error;
    }

    public UpdateMetadataResponse(Struct struct) {
        error = Errors.forCode(struct.get(ERROR_CODE));
    }

    public Errors error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    public static UpdateMetadataResponse parse(ByteBuffer buffer, short version) {
        return new UpdateMetadataResponse(ApiKeys.UPDATE_METADATA.parseResponse(version, buffer));
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.UPDATE_METADATA.responseSchema(version));
        struct.set(ERROR_CODE, error.code());
        return struct;
    }
}
