package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.config.ConfigResource;
import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.ArrayOf;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.*;

import static com.jackniu.kafka.common.protocol.CommonFields.*;
import static com.jackniu.kafka.common.protocol.types.Type.INT8;
import static com.jackniu.kafka.common.protocol.types.Type.STRING;

public class AlterConfigsResponse extends AbstractResponse {

    private static final String RESOURCES_KEY_NAME = "resources";
    private static final String RESOURCE_TYPE_KEY_NAME = "resource_type";
    private static final String RESOURCE_NAME_KEY_NAME = "resource_name";

    private static final Schema ALTER_CONFIGS_RESPONSE_ENTITY_V0 = new Schema(
            ERROR_CODE,
            ERROR_MESSAGE,
            new Field(RESOURCE_TYPE_KEY_NAME, INT8),
            new Field(RESOURCE_NAME_KEY_NAME, STRING));

    private static final Schema ALTER_CONFIGS_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            new Field(RESOURCES_KEY_NAME, new ArrayOf(ALTER_CONFIGS_RESPONSE_ENTITY_V0)));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema ALTER_CONFIGS_RESPONSE_V1 = ALTER_CONFIGS_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{ALTER_CONFIGS_RESPONSE_V0, ALTER_CONFIGS_RESPONSE_V1};
    }

    private final int throttleTimeMs;
    private final Map<ConfigResource, ApiError> errors;

    public AlterConfigsResponse(int throttleTimeMs, Map<ConfigResource, ApiError> errors) {
        this.throttleTimeMs = throttleTimeMs;
        this.errors = Objects.requireNonNull(errors, "errors");
    }

    public AlterConfigsResponse(Struct struct) {
        throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        Object[] resourcesArray = struct.getArray(RESOURCES_KEY_NAME);
        errors = new HashMap<>(resourcesArray.length);
        for (Object resourceObj : resourcesArray) {
            Struct resourceStruct = (Struct) resourceObj;
            ApiError error = new ApiError(resourceStruct);
            ConfigResource.Type resourceType = ConfigResource.Type.forId(resourceStruct.getByte(RESOURCE_TYPE_KEY_NAME));
            String resourceName = resourceStruct.getString(RESOURCE_NAME_KEY_NAME);
            errors.put(new ConfigResource(resourceType, resourceName), error);
        }
    }

    public Map<ConfigResource, ApiError> errors() {
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

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.ALTER_CONFIGS.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        List<Struct> resourceStructs = new ArrayList<>(errors.size());
        for (Map.Entry<ConfigResource, ApiError> entry : errors.entrySet()) {
            Struct resourceStruct = struct.instance(RESOURCES_KEY_NAME);
            ConfigResource resource = entry.getKey();
            entry.getValue().write(resourceStruct);
            resourceStruct.set(RESOURCE_TYPE_KEY_NAME, resource.type().id());
            resourceStruct.set(RESOURCE_NAME_KEY_NAME, resource.name());
            resourceStructs.add(resourceStruct);
        }
        struct.set(RESOURCES_KEY_NAME, resourceStructs.toArray(new Struct[0]));
        return struct;
    }

    public static AlterConfigsResponse parse(ByteBuffer buffer, short version) {
        return new AlterConfigsResponse(ApiKeys.ALTER_CONFIGS.parseResponse(version, buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
