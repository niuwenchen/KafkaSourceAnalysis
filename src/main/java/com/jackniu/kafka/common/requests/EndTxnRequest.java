package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static com.jackniu.kafka.common.protocol.CommonFields.*;
import static com.jackniu.kafka.common.protocol.types.Type.BOOLEAN;

public class EndTxnRequest extends AbstractRequest {
    private static final String TRANSACTION_RESULT_KEY_NAME = "transaction_result";

    private static final Schema END_TXN_REQUEST_V0 = new Schema(
            TRANSACTIONAL_ID,
            PRODUCER_ID,
            PRODUCER_EPOCH,
            new Field(TRANSACTION_RESULT_KEY_NAME, BOOLEAN, "The result of the transaction (0 = ABORT, 1 = COMMIT)"));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema END_TXN_REQUEST_V1 = END_TXN_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{END_TXN_REQUEST_V0, END_TXN_REQUEST_V1};
    }

    public static class Builder extends AbstractRequest.Builder<EndTxnRequest> {
        private final String transactionalId;
        private final long producerId;
        private final short producerEpoch;
        private final TransactionResult result;

        public Builder(String transactionalId, long producerId, short producerEpoch, TransactionResult result) {
            super(ApiKeys.END_TXN);
            this.transactionalId = transactionalId;
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.result = result;
        }

        public TransactionResult result() {
            return result;
        }

        @Override
        public EndTxnRequest build(short version) {
            return new EndTxnRequest(version, transactionalId, producerId, producerEpoch, result);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=EndTxnRequest").
                    append(", transactionalId=").append(transactionalId).
                    append(", producerId=").append(producerId).
                    append(", producerEpoch=").append(producerEpoch).
                    append(", result=").append(result).
                    append(")");
            return bld.toString();
        }
    }

    private final String transactionalId;
    private final long producerId;
    private final short producerEpoch;
    private final TransactionResult result;

    private EndTxnRequest(short version, String transactionalId, long producerId, short producerEpoch, TransactionResult result) {
        super(ApiKeys.END_TXN, version);
        this.transactionalId = transactionalId;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.result = result;
    }

    public EndTxnRequest(Struct struct, short version) {
        super(ApiKeys.END_TXN, version);
        this.transactionalId = struct.get(TRANSACTIONAL_ID);
        this.producerId = struct.get(PRODUCER_ID);
        this.producerEpoch = struct.get(PRODUCER_EPOCH);
        this.result = TransactionResult.forId(struct.getBoolean(TRANSACTION_RESULT_KEY_NAME));
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

    public TransactionResult command() {
        return result;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.END_TXN.requestSchema(version()));
        struct.set(TRANSACTIONAL_ID, transactionalId);
        struct.set(PRODUCER_ID, producerId);
        struct.set(PRODUCER_EPOCH, producerEpoch);
        struct.set(TRANSACTION_RESULT_KEY_NAME, result.id);
        return struct;
    }

    @Override
    public EndTxnResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new EndTxnResponse(throttleTimeMs, Errors.forException(e));
    }

    public static EndTxnRequest parse(ByteBuffer buffer, short version) {
        return new EndTxnRequest(ApiKeys.END_TXN.parseRequest(version, buffer), version);
    }

}

