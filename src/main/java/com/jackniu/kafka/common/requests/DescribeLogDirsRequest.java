package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.TopicPartition;
import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.types.ArrayOf;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.*;

import static com.jackniu.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static com.jackniu.kafka.common.protocol.types.Type.INT32;

public class DescribeLogDirsRequest  extends AbstractRequest {

    // request level key names
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level key names
    private static final String PARTITIONS_KEY_NAME = "partitions";

    private static final Schema DESCRIBE_LOG_DIRS_REQUEST_V0 = new Schema(
            new Field(TOPICS_KEY_NAME, ArrayOf.nullable(new Schema(
                    TOPIC_NAME,
                    new Field(PARTITIONS_KEY_NAME, new ArrayOf(INT32), "List of partition ids of the topic.")))));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema DESCRIBE_LOG_DIRS_REQUEST_V1 = DESCRIBE_LOG_DIRS_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{DESCRIBE_LOG_DIRS_REQUEST_V0, DESCRIBE_LOG_DIRS_REQUEST_V1};
    }

    private final Set<TopicPartition> topicPartitions;

    public static class Builder extends AbstractRequest.Builder<DescribeLogDirsRequest> {
        private final Set<TopicPartition> topicPartitions;

        // topicPartitions == null indicates requesting all partitions, and an empty list indicates requesting no partitions.
        public Builder(Set<TopicPartition> partitions) {
            super(ApiKeys.DESCRIBE_LOG_DIRS);
            this.topicPartitions = partitions;
        }

        @Override
        public DescribeLogDirsRequest build(short version) {
            return new DescribeLogDirsRequest(topicPartitions, version);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("(type=DescribeLogDirsRequest")
                    .append(", topicPartitions=")
                    .append(topicPartitions)
                    .append(")");
            return builder.toString();
        }
    }

    public DescribeLogDirsRequest(Struct struct, short version) {
        super(ApiKeys.DESCRIBE_LOG_DIRS, version);

        if (struct.getArray(TOPICS_KEY_NAME) == null) {
            topicPartitions = null;
        } else {
            topicPartitions = new HashSet<>();
            for (Object topicStructObj : struct.getArray(TOPICS_KEY_NAME)) {
                Struct topicStruct = (Struct) topicStructObj;
                String topic = topicStruct.get(TOPIC_NAME);
                for (Object partitionObj : topicStruct.getArray(PARTITIONS_KEY_NAME)) {
                    int partition = (Integer) partitionObj;
                    topicPartitions.add(new TopicPartition(topic, partition));
                }
            }
        }
    }

    // topicPartitions == null indicates requesting all partitions, and an empty list indicates requesting no partitions.
    public DescribeLogDirsRequest(Set<TopicPartition> topicPartitions, short version) {
        super(ApiKeys.DESCRIBE_LOG_DIRS, version);
        this.topicPartitions = topicPartitions;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.DESCRIBE_LOG_DIRS.requestSchema(version()));
        if (topicPartitions == null) {
            struct.set(TOPICS_KEY_NAME, null);
            return struct;
        }

        Map<String, List<Integer>> partitionsByTopic = new HashMap<>();
        for (TopicPartition tp : topicPartitions) {
            if (!partitionsByTopic.containsKey(tp.topic())) {
                partitionsByTopic.put(tp.topic(), new ArrayList<Integer>());
            }
            partitionsByTopic.get(tp.topic()).add(tp.partition());
        }

        List<Struct> topicStructArray = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> partitionsByTopicEntry : partitionsByTopic.entrySet()) {
            Struct topicStruct = struct.instance(TOPICS_KEY_NAME);
            topicStruct.set(TOPIC_NAME, partitionsByTopicEntry.getKey());
            topicStruct.set(PARTITIONS_KEY_NAME, partitionsByTopicEntry.getValue().toArray());
            topicStructArray.add(topicStruct);
        }
        struct.set(TOPICS_KEY_NAME, topicStructArray.toArray());

        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new DescribeLogDirsResponse(throttleTimeMs, new HashMap<String, DescribeLogDirsResponse.LogDirInfo>());
            default:
                throw new IllegalArgumentException(
                        String.format("Version %d is not valid. Valid versions for %s are 0 to %d", versionId,
                                this.getClass().getSimpleName(), ApiKeys.DESCRIBE_LOG_DIRS.latestVersion()));
        }
    }

    public boolean isAllTopicPartitions() {
        return topicPartitions == null;
    }

    public Set<TopicPartition> topicPartitions() {
        return topicPartitions;
    }

    public static DescribeLogDirsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeLogDirsRequest(ApiKeys.DESCRIBE_LOG_DIRS.parseRequest(version, buffer), version);
    }
}

