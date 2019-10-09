package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static com.jackniu.kafka.common.protocol.CommonFields.*;

public class AddOffsetsToTxnRequest  extends AbstractRequest {
    private static final Schema ADD_OFFSETS_TO_TXN_REQUEST_V0 = new Schema(
            TRANSACTIONAL_ID,
            PRODUCER_ID,
            PRODUCER_EPOCH,
            GROUP_ID);

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema ADD_OFFSETS_TO_TXN_REQUEST_V1 = ADD_OFFSETS_TO_TXN_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{ADD_OFFSETS_TO_TXN_REQUEST_V0, ADD_OFFSETS_TO_TXN_REQUEST_V1};
    }

    public static class Builder extends AbstractRequest.Builder<AddOffsetsToTxnRequest> {
        private final String transactionalId;
        private final long producerId;
        private final short producerEpoch;
        private final String consumerGroupId;

        public Builder(String transactionalId, long producerId, short producerEpoch, String consumerGroupId) {
            super(ApiKeys.ADD_OFFSETS_TO_TXN);
            this.transactionalId = transactionalId;
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.consumerGroupId = consumerGroupId;
        }

        public String consumerGroupId() {
            return consumerGroupId;
        }

        @Override
        public AddOffsetsToTxnRequest build(short version) {
            return new AddOffsetsToTxnRequest(version, transactionalId, producerId, producerEpoch, consumerGroupId);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=AddOffsetsToTxnRequest").
                    append(", transactionalId=").append(transactionalId).
                    append(", producerId=").append(producerId).
                    append(", producerEpoch=").append(producerEpoch).
                    append(", consumerGroupId=").append(consumerGroupId).
                    append(")");
            return bld.toString();
        }
    }

    private final String transactionalId;
    private final long producerId;
    private final short producerEpoch;
    private final String consumerGroupId;

    private AddOffsetsToTxnRequest(short version, String transactionalId, long producerId, short producerEpoch, String consumerGroupId) {
        super(ApiKeys.ADD_OFFSETS_TO_TXN, version);
        this.transactionalId = transactionalId;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.consumerGroupId = consumerGroupId;
    }

    public AddOffsetsToTxnRequest(Struct struct, short version) {
        super(ApiKeys.ADD_OFFSETS_TO_TXN, version);
        this.transactionalId = struct.get(TRANSACTIONAL_ID);
        this.producerId = struct.get(PRODUCER_ID);
        this.producerEpoch = struct.get(PRODUCER_EPOCH);
        this.consumerGroupId = struct.get(GROUP_ID);
    }

    public String transactionalId() {
        return transactionalId;
    }

    public long producerId() {
        return producerId;
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public String consumerGroupId() {
        return consumerGroupId;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.ADD_OFFSETS_TO_TXN.requestSchema(version()));
        struct.set(TRANSACTIONAL_ID, transactionalId);
        struct.set(PRODUCER_ID, producerId);
        struct.set(PRODUCER_EPOCH, producerEpoch);
        struct.set(GROUP_ID, consumerGroupId);
        return struct;
    }

    @Override
    public AddOffsetsToTxnResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new AddOffsetsToTxnResponse(throttleTimeMs, Errors.forException(e));
    }

    public static AddOffsetsToTxnRequest parse(ByteBuffer buffer, short version) {
        return new AddOffsetsToTxnRequest(ApiKeys.ADD_OFFSETS_TO_TXN.parseRequest(version, buffer), version);
    }

}

