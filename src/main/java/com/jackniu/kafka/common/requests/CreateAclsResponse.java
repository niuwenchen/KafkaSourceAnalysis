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

public class CreateAclsResponse extends AbstractResponse {
    private final static String CREATION_RESPONSES_KEY_NAME = "creation_responses";

    private static final Schema CREATE_ACLS_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            new Field(CREATION_RESPONSES_KEY_NAME, new ArrayOf(new Schema(
                    ERROR_CODE,
                    ERROR_MESSAGE))));

    /**
     * The version number is bumped to indicate that, on quota violation, brokers send out responses before throttling.
     */
    private static final Schema CREATE_ACLS_RESPONSE_V1 = CREATE_ACLS_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{CREATE_ACLS_RESPONSE_V0, CREATE_ACLS_RESPONSE_V1};
    }

    public static class AclCreationResponse {
        private final ApiError error;

        public AclCreationResponse(ApiError error) {
            this.error = error;
        }

        public ApiError error() {
            return error;
        }

        @Override
        public String toString() {
            return "(" + error + ")";
        }
    }

    private final int throttleTimeMs;

    private final List<AclCreationResponse> aclCreationResponses;

    public CreateAclsResponse(int throttleTimeMs, List<AclCreationResponse> aclCreationResponses) {
        this.throttleTimeMs = throttleTimeMs;
        this.aclCreationResponses = aclCreationResponses;
    }

    public CreateAclsResponse(Struct struct) {
        this.throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        this.aclCreationResponses = new ArrayList<>();
        for (Object responseStructObj : struct.getArray(CREATION_RESPONSES_KEY_NAME)) {
            Struct responseStruct = (Struct) responseStructObj;
            ApiError error = new ApiError(responseStruct);
            this.aclCreationResponses.add(new AclCreationResponse(error));
        }
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.CREATE_ACLS.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        List<Struct> responseStructs = new ArrayList<>();
        for (AclCreationResponse response : aclCreationResponses) {
            Struct responseStruct = struct.instance(CREATION_RESPONSES_KEY_NAME);
            response.error.write(responseStruct);
            responseStructs.add(responseStruct);
        }
        struct.set(CREATION_RESPONSES_KEY_NAME, responseStructs.toArray());
        return struct;
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public List<AclCreationResponse> aclCreationResponses() {
        return aclCreationResponses;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (AclCreationResponse response : aclCreationResponses)
            updateErrorCounts(errorCounts, response.error.error());
        return errorCounts;
    }

    public static CreateAclsResponse parse(ByteBuffer buffer, short version) {
        return new CreateAclsResponse(ApiKeys.CREATE_ACLS.responseSchema(version).read(buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}

