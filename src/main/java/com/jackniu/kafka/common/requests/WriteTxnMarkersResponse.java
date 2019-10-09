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
import java.util.HashMap;
import java.util.Map;

import static com.jackniu.kafka.common.protocol.CommonFields.*;
import static com.jackniu.kafka.common.protocol.types.Type.INT64;

public class WriteTxnMarkersResponse  extends AbstractResponse {
    private static final String TXN_MARKERS_KEY_NAME = "transaction_markers";

    private static final String PRODUCER_ID_KEY_NAME = "producer_id";
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    private static final Schema WRITE_TXN_MARKERS_PARTITION_ERROR_RESPONSE_V0 = new Schema(
            PARTITION_ID,
            ERROR_CODE);

    private static final Schema WRITE_TXN_MARKERS_ENTRY_V0 = new Schema(
            new Field(PRODUCER_ID_KEY_NAME, INT64, "Current producer id in use by the transactional id."),
            new Field(TOPICS_KEY_NAME, new ArrayOf(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITIONS_KEY_NAME, new ArrayOf(WRITE_TXN_MARKERS_PARTITION_ERROR_RESPONSE_V0)))),
                    "Errors per partition from writing markers."));

    private static final Schema WRITE_TXN_MARKERS_RESPONSE_V0 = new Schema(
            new Field(TXN_MARKERS_KEY_NAME, new ArrayOf(WRITE_TXN_MARKERS_ENTRY_V0), "Errors per partition from " +
                    "writing markers."));

    public static Schema[] schemaVersions() {
        return new Schema[]{WRITE_TXN_MARKERS_RESPONSE_V0};
    }

    // Possible error codes:
    //   CorruptRecord
    //   InvalidProducerEpoch
    //   UnknownTopicOrPartition
    //   NotLeaderForPartition
    //   MessageTooLarge
    //   RecordListTooLarge
    //   NotEnoughReplicas
    //   NotEnoughReplicasAfterAppend
    //   InvalidRequiredAcks
    //   TransactionCoordinatorFenced
    //   RequestTimeout
    //   ClusterAuthorizationFailed

    private final Map<Long, Map<TopicPartition, Errors>> errors;

    public WriteTxnMarkersResponse(Map<Long, Map<TopicPartition, Errors>> errors) {
        this.errors = errors;
    }

    public WriteTxnMarkersResponse(Struct struct) {
        Map<Long, Map<TopicPartition, Errors>> errors = new HashMap<>();

        Object[] responseArray = struct.getArray(TXN_MARKERS_KEY_NAME);
        for (Object responseObj : responseArray) {
            Struct responseStruct = (Struct) responseObj;

            long producerId = responseStruct.getLong(PRODUCER_ID_KEY_NAME);

            Map<TopicPartition, Errors> errorPerPartition = new HashMap<>();
            Object[] topicPartitionsArray = responseStruct.getArray(TOPICS_KEY_NAME);
            for (Object topicPartitionObj : topicPartitionsArray) {
                Struct topicPartitionStruct = (Struct) topicPartitionObj;
                String topic = topicPartitionStruct.get(TOPIC_NAME);
                for (Object partitionObj : topicPartitionStruct.getArray(PARTITIONS_KEY_NAME)) {
                    Struct partitionStruct = (Struct) partitionObj;
                    Integer partition = partitionStruct.get(PARTITION_ID);
                    Errors error = Errors.forCode(partitionStruct.get(ERROR_CODE));
                    errorPerPartition.put(new TopicPartition(topic, partition), error);
                }
            }
            errors.put(producerId, errorPerPartition);
        }

        this.errors = errors;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.WRITE_TXN_MARKERS.responseSchema(version));

        Object[] responsesArray = new Object[errors.size()];
        int k = 0;
        for (Map.Entry<Long, Map<TopicPartition, Errors>> responseEntry : errors.entrySet()) {
            Struct responseStruct = struct.instance(TXN_MARKERS_KEY_NAME);
            responseStruct.set(PRODUCER_ID_KEY_NAME, responseEntry.getKey());

            Map<TopicPartition, Errors> partitionAndErrors = responseEntry.getValue();
            Map<String, Map<Integer, Errors>> mappedPartitions = CollectionUtils.groupPartitionDataByTopic(partitionAndErrors);
            Object[] partitionsArray = new Object[mappedPartitions.size()];
            int i = 0;
            for (Map.Entry<String, Map<Integer, Errors>> topicAndPartitions : mappedPartitions.entrySet()) {
                Struct topicPartitionsStruct = responseStruct.instance(TOPICS_KEY_NAME);
                topicPartitionsStruct.set(TOPIC_NAME, topicAndPartitions.getKey());
                Map<Integer, Errors> partitionIdAndErrors = topicAndPartitions.getValue();

                Object[] partitionAndErrorsArray = new Object[partitionIdAndErrors.size()];
                int j = 0;
                for (Map.Entry<Integer, Errors> partitionAndError : partitionIdAndErrors.entrySet()) {
                    Struct partitionAndErrorStruct = topicPartitionsStruct.instance(PARTITIONS_KEY_NAME);
                    partitionAndErrorStruct.set(PARTITION_ID, partitionAndError.getKey());
                    partitionAndErrorStruct.set(ERROR_CODE, partitionAndError.getValue().code());
                    partitionAndErrorsArray[j++] = partitionAndErrorStruct;
                }
                topicPartitionsStruct.set(PARTITIONS_KEY_NAME, partitionAndErrorsArray);
                partitionsArray[i++] = topicPartitionsStruct;
            }
            responseStruct.set(TOPICS_KEY_NAME, partitionsArray);

            responsesArray[k++] = responseStruct;
        }

        struct.set(TXN_MARKERS_KEY_NAME, responsesArray);
        return struct;
    }

    public Map<TopicPartition, Errors> errors(long producerId) {
        return errors.get(producerId);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (Map<TopicPartition, Errors> allErrors : errors.values()) {
            for (Errors error : allErrors.values())
                updateErrorCounts(errorCounts, error);
        }
        return errorCounts;
    }

    public static WriteTxnMarkersResponse parse(ByteBuffer buffer, short version) {
        return new WriteTxnMarkersResponse(ApiKeys.WRITE_TXN_MARKERS.parseResponse(version, buffer));
    }
}

