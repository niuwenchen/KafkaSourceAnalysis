package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.TopicPartition;
import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.ArrayOf;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.jackniu.kafka.common.protocol.CommonFields.*;
import static com.jackniu.kafka.common.protocol.types.Type.INT32;

public class AddPartitionsToTxnRequest  extends AbstractRequest {
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    private static final Schema ADD_PARTITIONS_TO_TXN_REQUEST_V0 = new Schema(
            TRANSACTIONAL_ID,
            PRODUCER_ID,
            PRODUCER_EPOCH,
            new Field(TOPICS_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITIONS_KEY_NAME, new ArrayOf(INT32)))),
                    "The partitions to add to the transaction."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema ADD_PARTITIONS_TO_TXN_REQUEST_V1 = ADD_PARTITIONS_TO_TXN_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{ADD_PARTITIONS_TO_TXN_REQUEST_V0, ADD_PARTITIONS_TO_TXN_REQUEST_V1};
    }

    public static class Builder extends AbstractRequest.Builder<AddPartitionsToTxnRequest> {
        private final String transactionalId;
        private final long producerId;
        private final short producerEpoch;
        private final List<TopicPartition> partitions;

        public Builder(String transactionalId, long producerId, short producerEpoch, List<TopicPartition> partitions) {
            super(ApiKeys.ADD_PARTITIONS_TO_TXN);
            this.transactionalId = transactionalId;
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.partitions = partitions;
        }

        @Override
        public AddPartitionsToTxnRequest build(short version) {
            return new AddPartitionsToTxnRequest(version, transactionalId, producerId, producerEpoch, partitions);
        }

        public List<TopicPartition> partitions() {
            return partitions;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=AddPartitionsToTxnRequest").
                    append(", transactionalId=").append(transactionalId).
                    append(", producerId=").append(producerId).
                    append(", producerEpoch=").append(producerEpoch).
                    append(", partitions=").append(partitions).
                    append(")");
            return bld.toString();
        }
    }

    private final String transactionalId;
    private final long producerId;
    private final short producerEpoch;
    private final List<TopicPartition> partitions;

    private AddPartitionsToTxnRequest(short version, String transactionalId, long producerId, short producerEpoch,
                                      List<TopicPartition> partitions) {
        super(ApiKeys.ADD_PARTITIONS_TO_TXN, version);
        this.transactionalId = transactionalId;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.partitions = partitions;
    }

    public AddPartitionsToTxnRequest(Struct struct, short version) {
        super(ApiKeys.ADD_PARTITIONS_TO_TXN, version);
        this.transactionalId = struct.get(TRANSACTIONAL_ID);
        this.producerId = struct.get(PRODUCER_ID);
        this.producerEpoch = struct.get(PRODUCER_EPOCH);

        List<TopicPartition> partitions = new ArrayList<>();
        Object[] topicPartitionsArray = struct.getArray(TOPICS_KEY_NAME);
        for (Object topicPartitionObj : topicPartitionsArray) {
            Struct topicPartitionStruct = (Struct) topicPartitionObj;
            String topic = topicPartitionStruct.get(TOPIC_NAME);
            for (Object partitionObj : topicPartitionStruct.getArray(PARTITIONS_KEY_NAME)) {
                partitions.add(new TopicPartition(topic, (Integer) partitionObj));
            }
        }
        this.partitions = partitions;
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

    public List<TopicPartition> partitions() {
        return partitions;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.ADD_PARTITIONS_TO_TXN.requestSchema(version()));
        struct.set(TRANSACTIONAL_ID, transactionalId);
        struct.set(PRODUCER_ID, producerId);
        struct.set(PRODUCER_EPOCH, producerEpoch);

        Map<String, List<Integer>> mappedPartitions = CollectionUtils.groupPartitionsByTopic(partitions);
        Object[] partitionsArray = new Object[mappedPartitions.size()];
        int i = 0;
        for (Map.Entry<String, List<Integer>> topicAndPartitions : mappedPartitions.entrySet()) {
            Struct topicPartitionsStruct = struct.instance(TOPICS_KEY_NAME);
            topicPartitionsStruct.set(TOPIC_NAME, topicAndPartitions.getKey());
            topicPartitionsStruct.set(PARTITIONS_KEY_NAME, topicAndPartitions.getValue().toArray());
            partitionsArray[i++] = topicPartitionsStruct;
        }

        struct.set(TOPICS_KEY_NAME, partitionsArray);
        return struct;
    }

    @Override
    public AddPartitionsToTxnResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        final HashMap<TopicPartition, Errors> errors = new HashMap<>();
        for (TopicPartition partition : partitions) {
            errors.put(partition, Errors.forException(e));
        }
        return new AddPartitionsToTxnResponse(throttleTimeMs, errors);
    }

    public static AddPartitionsToTxnRequest parse(ByteBuffer buffer, short version) {
        return new AddPartitionsToTxnRequest(ApiKeys.ADD_PARTITIONS_TO_TXN.parseRequest(version, buffer), version);
    }

}

