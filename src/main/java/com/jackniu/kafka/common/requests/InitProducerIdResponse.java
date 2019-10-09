package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.jackniu.kafka.common.protocol.CommonFields.*;

public class InitProducerIdResponse extends AbstractResponse {
    // Possible error codes:
    //   NotCoordinator
    //   CoordinatorNotAvailable
    //   CoordinatorLoadInProgress
    //   TransactionalIdAuthorizationFailed
    //   ClusterAuthorizationFailed

    private static final Schema INIT_PRODUCER_ID_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE,
            PRODUCER_ID,
            PRODUCER_EPOCH);

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema INIT_PRODUCER_ID_RESPONSE_V1 = INIT_PRODUCER_ID_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{INIT_PRODUCER_ID_RESPONSE_V0, INIT_PRODUCER_ID_RESPONSE_V1};
    }

    private final int throttleTimeMs;
    private final Errors error;
    private final long producerId;
    private final short epoch;

    public InitProducerIdResponse(int throttleTimeMs, Errors error, long producerId, short epoch) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.producerId = producerId;
        this.epoch = epoch;
    }

    public InitProducerIdResponse(Struct struct) {
        this.throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        this.error = Errors.forCode(struct.get(ERROR_CODE));
        this.producerId = struct.get(PRODUCER_ID);
        this.epoch = struct.get(PRODUCER_EPOCH);
    }

    public InitProducerIdResponse(int throttleTimeMs, Errors errors) {
        this(throttleTimeMs, errors, RecordBatch.NO_PRODUCER_ID, (short) 0);
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public long producerId() {
        return producerId;
    }

    public Errors error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    public short epoch() {
        return epoch;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.INIT_PRODUCER_ID.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        struct.set(PRODUCER_ID, producerId);
        struct.set(PRODUCER_EPOCH, epoch);
        struct.set(ERROR_CODE, error.code());
        return struct;
    }

    public static InitProducerIdResponse parse(ByteBuffer buffer, short version) {
        return new InitProducerIdResponse(ApiKeys.INIT_PRODUCER_ID.parseResponse(version, buffer));
    }

    @Override
    public String toString() {
        return "InitProducerIdResponse(" +
                "error=" + error +
                ", producerId=" + producerId +
                ", producerEpoch=" + epoch +
                ", throttleTimeMs=" + throttleTimeMs +
                ')';
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}

