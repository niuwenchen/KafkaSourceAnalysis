package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.TopicPartition;
import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.ArrayOf;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.*;

import static com.jackniu.kafka.common.protocol.CommonFields.*;

public class ControlledShutdownResponse extends AbstractResponse {

    private static final String PARTITIONS_REMAINING_KEY_NAME = "partitions_remaining";

    private static final Schema CONTROLLED_SHUTDOWN_PARTITION_V0 = new Schema(
            TOPIC_NAME,
            PARTITION_ID);

    private static final Schema CONTROLLED_SHUTDOWN_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            new Field(PARTITIONS_REMAINING_KEY_NAME, new ArrayOf(CONTROLLED_SHUTDOWN_PARTITION_V0), "The partitions " +
                    "that the broker still leads."));

    private static final Schema CONTROLLED_SHUTDOWN_RESPONSE_V1 = CONTROLLED_SHUTDOWN_RESPONSE_V0;
    private static final Schema CONTROLLED_SHUTDOWN_RESPONSE_V2 = CONTROLLED_SHUTDOWN_RESPONSE_V1;

    public static Schema[] schemaVersions() {
        return new Schema[]{CONTROLLED_SHUTDOWN_RESPONSE_V0, CONTROLLED_SHUTDOWN_RESPONSE_V1, CONTROLLED_SHUTDOWN_RESPONSE_V2};
    }

    /**
     * Possible error codes:
     *
     * UNKNOWN(-1) (this is because IllegalStateException may be thrown in `KafkaController.shutdownBroker`, it would be good to improve this)
     * BROKER_NOT_AVAILABLE(8)
     * STALE_CONTROLLER_EPOCH(11)
     */
    private final Errors error;

    private final Set<TopicPartition> partitionsRemaining;

    public ControlledShutdownResponse(Errors error, Set<TopicPartition> partitionsRemaining) {
        this.error = error;
        this.partitionsRemaining = partitionsRemaining;
    }

    public ControlledShutdownResponse(Struct struct) {
        error = Errors.forCode(struct.get(ERROR_CODE));
        Set<TopicPartition> partitions = new HashSet<>();
        for (Object topicPartitionObj : struct.getArray(PARTITIONS_REMAINING_KEY_NAME)) {
            Struct topicPartition = (Struct) topicPartitionObj;
            String topic = topicPartition.get(TOPIC_NAME);
            int partition = topicPartition.get(PARTITION_ID);
            partitions.add(new TopicPartition(topic, partition));
        }
        partitionsRemaining = partitions;
    }

    public Errors error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    public Set<TopicPartition> partitionsRemaining() {
        return partitionsRemaining;
    }

    public static ControlledShutdownResponse parse(ByteBuffer buffer, short version) {
        return new ControlledShutdownResponse(ApiKeys.CONTROLLED_SHUTDOWN.parseResponse(version, buffer));
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.CONTROLLED_SHUTDOWN.responseSchema(version));
        struct.set(ERROR_CODE, error.code());

        List<Struct> partitionsRemainingList = new ArrayList<>(partitionsRemaining.size());
        for (TopicPartition topicPartition : partitionsRemaining) {
            Struct topicPartitionStruct = struct.instance(PARTITIONS_REMAINING_KEY_NAME);
            topicPartitionStruct.set(TOPIC_NAME, topicPartition.topic());
            topicPartitionStruct.set(PARTITION_ID, topicPartition.partition());
            partitionsRemainingList.add(topicPartitionStruct);
        }
        struct.set(PARTITIONS_REMAINING_KEY_NAME, partitionsRemainingList.toArray());

        return struct;
    }
}

