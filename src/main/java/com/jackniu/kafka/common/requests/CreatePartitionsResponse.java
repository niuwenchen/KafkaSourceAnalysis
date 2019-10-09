package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.ArrayOf;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.jackniu.kafka.common.protocol.CommonFields.*;

public class CreatePartitionsResponse  extends AbstractResponse {

    private static final String TOPIC_ERRORS_KEY_NAME = "topic_errors";

    private static final Schema CREATE_PARTITIONS_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            new Field(TOPIC_ERRORS_KEY_NAME, new ArrayOf(
                    new Schema(
                            TOPIC_NAME,
                            ERROR_CODE,
                            ERROR_MESSAGE
                    )), "Per topic results for the create partitions request")
    );

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema CREATE_PARTITIONS_RESPONSE_V1 = CREATE_PARTITIONS_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{CREATE_PARTITIONS_RESPONSE_V0, CREATE_PARTITIONS_RESPONSE_V1};
    }

    private final int throttleTimeMs;
    private final Map<String, ApiError> errors;

    public CreatePartitionsResponse(int throttleTimeMs, Map<String, ApiError> errors) {
        this.throttleTimeMs = throttleTimeMs;
        this.errors = errors;
    }

    public CreatePartitionsResponse(Struct struct) {
        super();
        Object[] topicErrorsArray = struct.getArray(TOPIC_ERRORS_KEY_NAME);
        Map<String, ApiError> errors = new HashMap<>(topicErrorsArray.length);
        for (Object topicErrorObj : topicErrorsArray) {
            Struct topicErrorStruct = (Struct) topicErrorObj;
            String topic = topicErrorStruct.get(TOPIC_NAME);
            ApiError error = new ApiError(topicErrorStruct);
            errors.put(topic, error);
        }
        this.throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        this.errors = errors;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.CREATE_PARTITIONS.responseSchema(version));
        List<Struct> topicErrors = new ArrayList<>(errors.size());
        for (Map.Entry<String, ApiError> error : errors.entrySet()) {
            Struct errorStruct = struct.instance(TOPIC_ERRORS_KEY_NAME);
            errorStruct.set(TOPIC_NAME, error.getKey());
            error.getValue().write(errorStruct);
            topicErrors.add(errorStruct);
        }
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        struct.set(TOPIC_ERRORS_KEY_NAME, topicErrors.toArray(new Object[topicErrors.size()]));
        return struct;
    }

    public Map<String, ApiError> errors() {
        return errors;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return apiErrorCounts(errors);
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public static CreatePartitionsResponse parse(ByteBuffer buffer, short version) {
        return new CreatePartitionsResponse(ApiKeys.CREATE_PARTITIONS.parseResponse(version, buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}

