package com.jackniu.kafka.common.requests;


import com.jackniu.kafka.common.TopicPartition;
import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.record.RecordBatch;
import com.jackniu.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.jackniu.kafka.common.protocol.CommonFields.*;

/**
 * Possible error codes:
 *
 * - {@link Errors#TOPIC_AUTHORIZATION_FAILED} If the user does not have DESCRIBE access to a requested topic
 * - {@link Errors#REPLICA_NOT_AVAILABLE} If the request is received by a broker which is not a replica
 * - {@link Errors#NOT_LEADER_FOR_PARTITION} If the broker is not a leader and either the provided leader epoch
 *     matches the known leader epoch on the broker or is empty
 * - {@link Errors#FENCED_LEADER_EPOCH} If the epoch is lower than the broker's epoch
 * - {@link Errors#UNKNOWN_LEADER_EPOCH} If the epoch is larger than the broker's epoch
 * - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION} If the broker does not have metadata for a topic or partition
 * - {@link Errors#KAFKA_STORAGE_ERROR} If the log directory for one of the requested partitions is offline
 * - {@link Errors#UNKNOWN_SERVER_ERROR} For any unexpected errors
 */

public class OffsetsForLeaderEpochResponse extends AbstractResponse {
    private static final Field.ComplexArray TOPICS = new Field.ComplexArray("topics",
            "An array of topics for which we have leader offsets for some requested partition leader epoch");
    private static final Field.ComplexArray PARTITIONS = new Field.ComplexArray("partitions",
            "An array of offsets by partition");
    private static final Field.Int64 END_OFFSET = new Field.Int64("end_offset", "The end offset");

    private static final Field PARTITIONS_V0 = PARTITIONS.withFields(
            ERROR_CODE,
            PARTITION_ID,
            END_OFFSET);
    private static final Field TOPICS_V0 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V0);
    private static final Schema OFFSET_FOR_LEADER_EPOCH_RESPONSE_V0 = new Schema(
            TOPICS_V0);

    // V1 added a per-partition leader epoch field which specifies which leader epoch the end offset belongs to
    private static final Field PARTITIONS_V1 = PARTITIONS.withFields(
            ERROR_CODE,
            PARTITION_ID,
            LEADER_EPOCH,
            END_OFFSET);
    private static final Field TOPICS_V1 = TOPICS.withFields(
            TOPIC_NAME,
            PARTITIONS_V1);
    private static final Schema OFFSET_FOR_LEADER_EPOCH_RESPONSE_V1 = new Schema(
            TOPICS_V1);

    // V2 bumped for addition of current leader epoch to the request schema and the addition of the throttle
    // time in the response
    private static final Schema OFFSET_FOR_LEADER_EPOCH_RESPONSE_V2 = new Schema(
            THROTTLE_TIME_MS,
            TOPICS_V1);

    public static Schema[] schemaVersions() {
        return new Schema[]{OFFSET_FOR_LEADER_EPOCH_RESPONSE_V0, OFFSET_FOR_LEADER_EPOCH_RESPONSE_V1,
                OFFSET_FOR_LEADER_EPOCH_RESPONSE_V2};
    }

    private final int throttleTimeMs;
    private final Map<TopicPartition, EpochEndOffset> epochEndOffsetsByPartition;

    public OffsetsForLeaderEpochResponse(Struct struct) {
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        this.epochEndOffsetsByPartition = new HashMap<>();
        for (Object topicAndEpocsObj : struct.get(TOPICS)) {
            Struct topicAndEpochs = (Struct) topicAndEpocsObj;
            String topic = topicAndEpochs.get(TOPIC_NAME);
            for (Object partitionAndEpochObj : topicAndEpochs.get(PARTITIONS)) {
                Struct partitionAndEpoch = (Struct) partitionAndEpochObj;
                Errors error = Errors.forCode(partitionAndEpoch.get(ERROR_CODE));
                int partitionId = partitionAndEpoch.get(PARTITION_ID);
                TopicPartition tp = new TopicPartition(topic, partitionId);
                int leaderEpoch = partitionAndEpoch.getOrElse(LEADER_EPOCH, RecordBatch.NO_PARTITION_LEADER_EPOCH);
                long endOffset = partitionAndEpoch.get(END_OFFSET);
                epochEndOffsetsByPartition.put(tp, new EpochEndOffset(error, leaderEpoch, endOffset));
            }
        }
    }

    public OffsetsForLeaderEpochResponse(Map<TopicPartition, EpochEndOffset> epochsByTopic) {
        this(DEFAULT_THROTTLE_TIME, epochsByTopic);
    }

    public OffsetsForLeaderEpochResponse(int throttleTimeMs, Map<TopicPartition, EpochEndOffset> epochsByTopic) {
        this.throttleTimeMs = throttleTimeMs;
        this.epochEndOffsetsByPartition = epochsByTopic;
    }

    public Map<TopicPartition, EpochEndOffset> responses() {
        return epochEndOffsetsByPartition;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (EpochEndOffset response : epochEndOffsetsByPartition.values())
            updateErrorCounts(errorCounts, response.error());
        return errorCounts;
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public static OffsetsForLeaderEpochResponse parse(ByteBuffer buffer, short versionId) {
        return new OffsetsForLeaderEpochResponse(ApiKeys.OFFSET_FOR_LEADER_EPOCH.responseSchema(versionId).read(buffer));
    }

    @Override
    protected Struct toStruct(short version) {
        Struct responseStruct = new Struct(ApiKeys.OFFSET_FOR_LEADER_EPOCH.responseSchema(version));
        responseStruct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);

        Map<String, Map<Integer, EpochEndOffset>> endOffsetsByTopic = CollectionUtils.groupPartitionDataByTopic(epochEndOffsetsByPartition);
        List<Struct> topics = new ArrayList<>(endOffsetsByTopic.size());
        for (Map.Entry<String, Map<Integer, EpochEndOffset>> topicToPartitionEpochs : endOffsetsByTopic.entrySet()) {
            Struct topicStruct = responseStruct.instance(TOPICS);
            topicStruct.set(TOPIC_NAME, topicToPartitionEpochs.getKey());
            Map<Integer, EpochEndOffset> partitionEpochs = topicToPartitionEpochs.getValue();
            List<Struct> partitions = new ArrayList<>();
            for (Map.Entry<Integer, EpochEndOffset> partitionEndOffset : partitionEpochs.entrySet()) {
                Struct partitionStruct = topicStruct.instance(PARTITIONS);
                partitionStruct.set(ERROR_CODE, partitionEndOffset.getValue().error().code());
                partitionStruct.set(PARTITION_ID, partitionEndOffset.getKey());
                partitionStruct.setIfExists(LEADER_EPOCH, partitionEndOffset.getValue().leaderEpoch());
                partitionStruct.set(END_OFFSET, partitionEndOffset.getValue().endOffset());
                partitions.add(partitionStruct);
            }
            topicStruct.set(PARTITIONS, partitions.toArray());
            topics.add(topicStruct);
        }
        responseStruct.set(TOPICS, topics.toArray());
        return responseStruct;
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("(type=OffsetsForLeaderEpochResponse, ")
                .append(", throttleTimeMs=").append(throttleTimeMs)
                .append(", epochEndOffsetsByPartition=").append(epochEndOffsetsByPartition)
                .append(")");
        return bld.toString();
    }
}

