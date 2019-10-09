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

public class AddPartitionsToTxnResponse extends AbstractResponse {
    private static final String ERRORS_KEY_NAME = "errors";
    private static final String PARTITION_ERRORS = "partition_errors";

    private static final Schema ADD_PARTITIONS_TO_TXN_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            new Field(ERRORS_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITION_ERRORS, new ArrayOf(new Schema(
                            PARTITION_ID,
                            ERROR_CODE)))))));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema ADD_PARTITIONS_TO_TXN_RESPONSE_V1 = ADD_PARTITIONS_TO_TXN_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{ADD_PARTITIONS_TO_TXN_RESPONSE_V0, ADD_PARTITIONS_TO_TXN_RESPONSE_V1};
    }

    private final int throttleTimeMs;

    // Possible error codes:
    //   NotCoordinator
    //   CoordinatorNotAvailable
    //   CoordinatorLoadInProgress
    //   InvalidTxnState
    //   InvalidProducerIdMapping
    //   TopicAuthorizationFailed
    //   InvalidProducerEpoch
    //   UnknownTopicOrPartition
    //   TopicAuthorizationFailed
    //   TransactionalIdAuthorizationFailed
    private final Map<TopicPartition, Errors> errors;

    public AddPartitionsToTxnResponse(int throttleTimeMs, Map<TopicPartition, Errors> errors) {
        this.throttleTimeMs = throttleTimeMs;
        this.errors = errors;
    }

    public AddPartitionsToTxnResponse(Struct struct) {
        this.throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        errors = new HashMap<>();
        for (Object topic : struct.getArray(ERRORS_KEY_NAME)) {
            Struct topicStruct = (Struct) topic;
            final String topicName = topicStruct.get(TOPIC_NAME);
            for (Object partition : topicStruct.getArray(PARTITION_ERRORS)) {
                Struct partitionStruct = (Struct) partition;
                TopicPartition topicPartition = new TopicPartition(topicName, partitionStruct.get(PARTITION_ID));
                errors.put(topicPartition, Errors.forCode(partitionStruct.get(ERROR_CODE)));
            }
        }
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

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.ADD_PARTITIONS_TO_TXN.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);

        Map<String, Map<Integer, Errors>> errorsByTopic = CollectionUtils.groupPartitionDataByTopic(errors);
        List<Struct> topics = new ArrayList<>(errorsByTopic.size());
        for (Map.Entry<String, Map<Integer, Errors>> entry : errorsByTopic.entrySet()) {
            Struct topicErrorCodes = struct.instance(ERRORS_KEY_NAME);
            topicErrorCodes.set(TOPIC_NAME, entry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, Errors> partitionErrors : entry.getValue().entrySet()) {
                final Struct partitionData = topicErrorCodes.instance(PARTITION_ERRORS)
                        .set(PARTITION_ID, partitionErrors.getKey())
                        .set(ERROR_CODE, partitionErrors.getValue().code());
                partitionArray.add(partitionData);

            }
            topicErrorCodes.set(PARTITION_ERRORS, partitionArray.toArray());
            topics.add(topicErrorCodes);
        }
        struct.set(ERRORS_KEY_NAME, topics.toArray());
        return struct;
    }

    public static AddPartitionsToTxnResponse parse(ByteBuffer buffer, short version) {
        return new AddPartitionsToTxnResponse(ApiKeys.ADD_PARTITIONS_TO_TXN.parseResponse(version, buffer));
    }

    @Override
    public String toString() {
        return "AddPartitionsToTxnResponse(" +
                "errors=" + errors +
                ", throttleTimeMs=" + throttleTimeMs +
                ')';
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
