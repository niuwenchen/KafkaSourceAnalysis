package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.jackniu.kafka.common.protocol.CommonFields.ERROR_CODE;
import static com.jackniu.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;

public class AddOffsetsToTxnResponse  extends AbstractResponse {
    private static final Schema ADD_OFFSETS_TO_TXN_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE);

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema ADD_OFFSETS_TO_TXN_RESPONSE_V1 = ADD_OFFSETS_TO_TXN_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{ADD_OFFSETS_TO_TXN_RESPONSE_V0, ADD_OFFSETS_TO_TXN_RESPONSE_V1};
    }

    // Possible error codes:
    //   NotCoordinator
    //   CoordinatorNotAvailable
    //   CoordinatorLoadInProgress
    //   InvalidProducerIdMapping
    //   InvalidProducerEpoch
    //   InvalidTxnState
    //   GroupAuthorizationFailed
    //   TransactionalIdAuthorizationFailed

    private final Errors error;
    private final int throttleTimeMs;

    public AddOffsetsToTxnResponse(int throttleTimeMs, Errors error) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
    }

    public AddOffsetsToTxnResponse(Struct struct) {
        this.throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        this.error = Errors.forCode(struct.get(ERROR_CODE));
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
        Struct struct = new Struct(ApiKeys.ADD_OFFSETS_TO_TXN.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        struct.set(ERROR_CODE, error.code());
        return struct;
    }

    public static AddOffsetsToTxnResponse parse(ByteBuffer buffer, short version) {
        return new AddOffsetsToTxnResponse(ApiKeys.ADD_OFFSETS_TO_TXN.parseResponse(version, buffer));
    }

    @Override
    public String toString() {
        return "AddOffsetsToTxnResponse(" +
                "error=" + error +
                ", throttleTimeMs=" + throttleTimeMs +
                ')';
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}

