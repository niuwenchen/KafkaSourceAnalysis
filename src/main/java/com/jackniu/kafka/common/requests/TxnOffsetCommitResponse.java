package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.TopicPartition;
import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static com.jackniu.kafka.common.protocol.CommonFields.*;

/**
 *
 * Possible error codes:
 *   InvalidProducerEpoch
 *   NotCoordinator
 *   CoordinatorNotAvailable
 *   CoordinatorLoadInProgress
 *   OffsetMetadataTooLarge
 *   GroupAuthorizationFailed
 *   InvalidCommitOffsetSize
 *   TransactionalIdAuthorizationFailed
 *   RequestTimedOut
 */
public class TxnOffsetCommitResponse extends AbstractResponse {
    private static final Field.ComplexArray TOPICS = new Field.ComplexArray("topics",
            "Responses by topic for committed offsets");

    // topic level fields
    private static final Field.ComplexArray PARTITIONS = new Field.ComplexArray("partitions",
            "Responses by partition for committed offsets");

    private static final Field PARTITIONS_V0 = PARTITIONS.withFields(
            PARTITION_ID,
            ERROR_CODE);

    private static final Field TOPICS_V0 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V0);

    private static final Schema TXN_OFFSET_COMMIT_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            TOPICS_V0);

    // V1 bump used to indicate that on quota violation brokers send out responses before throttling.
    private static final Schema TXN_OFFSET_COMMIT_RESPONSE_V1 = TXN_OFFSET_COMMIT_RESPONSE_V0;

    // V2 adds the leader epoch to the partition data
    private static final Schema TXN_OFFSET_COMMIT_RESPONSE_V2 = TXN_OFFSET_COMMIT_RESPONSE_V1;

    public static Schema[] schemaVersions() {
        return new Schema[]{TXN_OFFSET_COMMIT_RESPONSE_V0, TXN_OFFSET_COMMIT_RESPONSE_V1, TXN_OFFSET_COMMIT_RESPONSE_V2};
    }

    private final Map<TopicPartition, Errors> errors;
    private final int throttleTimeMs;

    public TxnOffsetCommitResponse(int throttleTimeMs, Map<TopicPartition, Errors> errors) {
        this.throttleTimeMs = throttleTimeMs;
        this.errors = errors;
    }

    public TxnOffsetCommitResponse(Struct struct) {
        this.throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        Map<TopicPartition, Errors> errors = new HashMap<>();
        Object[] topicPartitionsArray = struct.get(TOPICS);
        for (Object topicPartitionObj : topicPartitionsArray) {
            Struct topicPartitionStruct = (Struct) topicPartitionObj;
            String topic = topicPartitionStruct.get(TOPIC_NAME);
            for (Object partitionObj : topicPartitionStruct.get(PARTITIONS)) {
                Struct partitionStruct = (Struct) partitionObj;
                Integer partition = partitionStruct.get(PARTITION_ID);
                Errors error = Errors.forCode(partitionStruct.get(ERROR_CODE));
                errors.put(new TopicPartition(topic, partition), error);
            }
        }
        this.errors = errors;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.TXN_OFFSET_COMMIT.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        Map<String, Map<Integer, Errors>> mappedPartitions = CollectionUtils.groupPartitionDataByTopic(errors);
        Object[] partitionsArray = new Object[mappedPartitions.size()];
        int i = 0;
        for (Map.Entry<String, Map<Integer, Errors>> topicAndPartitions : mappedPartitions.entrySet()) {
            Struct topicPartitionsStruct = struct.instance(TOPICS);
            topicPartitionsStruct.set(TOPIC_NAME, topicAndPartitions.getKey());
            Map<Integer, Errors> partitionAndErrors = topicAndPartitions.getValue();

            Object[] partitionAndErrorsArray = new Object[partitionAndErrors.size()];
            int j = 0;
            for (Map.Entry<Integer, Errors> partitionAndError : partitionAndErrors.entrySet()) {
                Struct partitionAndErrorStruct = topicPartitionsStruct.instance(PARTITIONS);
                partitionAndErrorStruct.set(PARTITION_ID, partitionAndError.getKey());
                partitionAndErrorStruct.set(ERROR_CODE, partitionAndError.getValue().code());
                partitionAndErrorsArray[j++] = partitionAndErrorStruct;
            }
            topicPartitionsStruct.set(PARTITIONS, partitionAndErrorsArray);
            partitionsArray[i++] = topicPartitionsStruct;
        }

        struct.set(TOPICS, partitionsArray);
        return struct;
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<TopicPartition, Errors> errors() {
        return errors;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(errors);
    }

    public static TxnOffsetCommitResponse parse(ByteBuffer buffer, short version) {
        return new TxnOffsetCommitResponse(ApiKeys.TXN_OFFSET_COMMIT.parseResponse(version, buffer));
    }

    @Override
    public String toString() {
        return "TxnOffsetCommitResponse(" +
                "errors=" + errors +
                ", throttleTimeMs=" + throttleTimeMs +
                ')';
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
