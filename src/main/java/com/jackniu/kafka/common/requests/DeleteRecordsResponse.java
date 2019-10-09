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
import static com.jackniu.kafka.common.protocol.types.Type.INT64;

public class DeleteRecordsResponse extends AbstractResponse {

    public static final long INVALID_LOW_WATERMARK = -1L;

    // request level key names
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level key names
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level key names
    private static final String LOW_WATERMARK_KEY_NAME = "low_watermark";

    private static final Schema DELETE_RECORDS_RESPONSE_PARTITION_V0 = new Schema(
            PARTITION_ID,
            new Field(LOW_WATERMARK_KEY_NAME, INT64, "Smallest available offset of all live replicas"),
            ERROR_CODE);

    private static final Schema DELETE_RECORDS_RESPONSE_TOPIC_V0 = new Schema(
            TOPIC_NAME,
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(DELETE_RECORDS_RESPONSE_PARTITION_V0)));

    private static final Schema DELETE_RECORDS_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            new Field(TOPICS_KEY_NAME, new ArrayOf(DELETE_RECORDS_RESPONSE_TOPIC_V0)));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema DELETE_RECORDS_RESPONSE_V1 = DELETE_RECORDS_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{DELETE_RECORDS_RESPONSE_V0, DELETE_RECORDS_RESPONSE_V1};
    }

    private final int throttleTimeMs;
    private final Map<TopicPartition, PartitionResponse> responses;

    /**
     * Possible error code:
     *
     * OFFSET_OUT_OF_RANGE (1)
     * UNKNOWN_TOPIC_OR_PARTITION (3)
     * NOT_LEADER_FOR_PARTITION (6)
     * REQUEST_TIMED_OUT (7)
     * UNKNOWN (-1)
     */

    public static final class PartitionResponse {
        public long lowWatermark;
        public Errors error;

        public PartitionResponse(long lowWatermark, Errors error) {
            this.lowWatermark = lowWatermark;
            this.error = error;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append('{')
                    .append(",low_watermark: ")
                    .append(lowWatermark)
                    .append("error: ")
                    .append(error.toString())
                    .append('}');
            return builder.toString();
        }
    }

    public DeleteRecordsResponse(Struct struct) {
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        responses = new HashMap<>();
        for (Object topicStructObj : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicStruct = (Struct) topicStructObj;
            String topic = topicStruct.get(TOPIC_NAME);
            for (Object partitionStructObj : topicStruct.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionStruct = (Struct) partitionStructObj;
                int partition = partitionStruct.get(PARTITION_ID);
                long lowWatermark = partitionStruct.getLong(LOW_WATERMARK_KEY_NAME);
                Errors error = Errors.forCode(partitionStruct.get(ERROR_CODE));
                responses.put(new TopicPartition(topic, partition), new PartitionResponse(lowWatermark, error));
            }
        }
    }

    /**
     * Constructor for version 0.
     */
    public DeleteRecordsResponse(int throttleTimeMs, Map<TopicPartition, PartitionResponse> responses) {
        this.throttleTimeMs = throttleTimeMs;
        this.responses = responses;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.DELETE_RECORDS.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        Map<String, Map<Integer, PartitionResponse>> responsesByTopic = CollectionUtils.groupPartitionDataByTopic(responses);
        List<Struct> topicStructArray = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, PartitionResponse>> responsesByTopicEntry : responsesByTopic.entrySet()) {
            Struct topicStruct = struct.instance(TOPICS_KEY_NAME);
            topicStruct.set(TOPIC_NAME, responsesByTopicEntry.getKey());
            List<Struct> partitionStructArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionResponse> responsesByPartitionEntry : responsesByTopicEntry.getValue().entrySet()) {
                Struct partitionStruct = topicStruct.instance(PARTITIONS_KEY_NAME);
                PartitionResponse response = responsesByPartitionEntry.getValue();
                partitionStruct.set(PARTITION_ID, responsesByPartitionEntry.getKey());
                partitionStruct.set(LOW_WATERMARK_KEY_NAME, response.lowWatermark);
                partitionStruct.set(ERROR_CODE, response.error.code());
                partitionStructArray.add(partitionStruct);
            }
            topicStruct.set(PARTITIONS_KEY_NAME, partitionStructArray.toArray());
            topicStructArray.add(topicStruct);
        }
        struct.set(TOPICS_KEY_NAME, topicStructArray.toArray());
        return struct;
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<TopicPartition, PartitionResponse> responses() {
        return this.responses;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (PartitionResponse response : responses.values())
            updateErrorCounts(errorCounts, response.error);
        return errorCounts;
    }

    public static DeleteRecordsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteRecordsResponse(ApiKeys.DELETE_RECORDS.responseSchema(version).read(buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
