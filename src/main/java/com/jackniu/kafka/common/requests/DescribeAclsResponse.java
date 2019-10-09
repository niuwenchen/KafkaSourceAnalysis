package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.acl.AccessControlEntry;
import com.jackniu.kafka.common.acl.AclBinding;
import com.jackniu.kafka.common.errors.UnsupportedVersionException;
import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.ArrayOf;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.resource.PatternType;
import com.jackniu.kafka.common.resource.ResourcePattern;

import java.nio.ByteBuffer;
import java.util.*;

import static com.jackniu.kafka.common.protocol.CommonFields.*;

public class DescribeAclsResponse extends AbstractResponse {
    private final static String RESOURCES_KEY_NAME = "resources";
    private final static String ACLS_KEY_NAME = "acls";

    private static final Schema DESCRIBE_ACLS_RESOURCE_V0 = new Schema(
            RESOURCE_TYPE,
            RESOURCE_NAME,
            new Field(ACLS_KEY_NAME, new ArrayOf(new Schema(
                    PRINCIPAL,
                    HOST,
                    OPERATION,
                    PERMISSION_TYPE))));

    /**
     * V1 sees a new `RESOURCE_PATTERN_TYPE` that defines the type of the resource pattern.
     *
     * For more info, see {@link PatternType}.
     */
    private static final Schema DESCRIBE_ACLS_RESOURCE_V1 = new Schema(
            RESOURCE_TYPE,
            RESOURCE_NAME,
            RESOURCE_PATTERN_TYPE,
            new Field(ACLS_KEY_NAME, new ArrayOf(new Schema(
                    PRINCIPAL,
                    HOST,
                    OPERATION,
                    PERMISSION_TYPE))));

    private static final Schema DESCRIBE_ACLS_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE,
            ERROR_MESSAGE,
            new Field(RESOURCES_KEY_NAME, new ArrayOf(DESCRIBE_ACLS_RESOURCE_V0), "The resources and their associated ACLs."));

    /**
     * V1 sees a new `RESOURCE_PATTERN_TYPE` field added to DESCRIBE_ACLS_RESOURCE_V1, that describes how the resource name is interpreted
     * and version was bumped to indicate that, on quota violation, brokers send out responses before throttling.
     *
     * For more info, see {@link PatternType}.
     */
    private static final Schema DESCRIBE_ACLS_RESPONSE_V1 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE,
            ERROR_MESSAGE,
            new Field(RESOURCES_KEY_NAME, new ArrayOf(DESCRIBE_ACLS_RESOURCE_V1), "The resources and their associated ACLs."));

    public static Schema[] schemaVersions() {
        return new Schema[]{DESCRIBE_ACLS_RESPONSE_V0, DESCRIBE_ACLS_RESPONSE_V1};
    }

    private final int throttleTimeMs;
    private final ApiError error;
    private final Collection<AclBinding> acls;

    public DescribeAclsResponse(int throttleTimeMs, ApiError error, Collection<AclBinding> acls) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.acls = acls;
    }

    public DescribeAclsResponse(Struct struct) {
        this.throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        this.error = new ApiError(struct);
        this.acls = new ArrayList<>();
        for (Object resourceStructObj : struct.getArray(RESOURCES_KEY_NAME)) {
            Struct resourceStruct = (Struct) resourceStructObj;
            ResourcePattern pattern = RequestUtils.resourcePatternromStructFields(resourceStruct);
            for (Object aclDataStructObj : resourceStruct.getArray(ACLS_KEY_NAME)) {
                Struct aclDataStruct = (Struct) aclDataStructObj;
                AccessControlEntry entry = RequestUtils.aceFromStructFields(aclDataStruct);
                this.acls.add(new AclBinding(pattern, entry));
            }
        }
    }

    @Override
    protected Struct toStruct(short version) {
        validate(version);

        Struct struct = new Struct(ApiKeys.DESCRIBE_ACLS.responseSchema(version));
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        error.write(struct);

        Map<ResourcePattern, List<AccessControlEntry>> resourceToData = new HashMap<>();
        for (AclBinding acl : acls) {
            resourceToData
                    .computeIfAbsent(acl.pattern(), k -> new ArrayList<>())
                    .add(acl.entry());
        }

        List<Struct> resourceStructs = new ArrayList<>();
        for (Map.Entry<ResourcePattern, List<AccessControlEntry>> tuple : resourceToData.entrySet()) {
            ResourcePattern resource = tuple.getKey();
            Struct resourceStruct = struct.instance(RESOURCES_KEY_NAME);
            RequestUtils.resourcePatternSetStructFields(resource, resourceStruct);
            List<Struct> dataStructs = new ArrayList<>();
            for (AccessControlEntry entry : tuple.getValue()) {
                Struct dataStruct = resourceStruct.instance(ACLS_KEY_NAME);
                RequestUtils.aceSetStructFields(entry, dataStruct);
                dataStructs.add(dataStruct);
            }
            resourceStruct.set(ACLS_KEY_NAME, dataStructs.toArray());
            resourceStructs.add(resourceStruct);
        }
        struct.set(RESOURCES_KEY_NAME, resourceStructs.toArray());
        return struct;
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public ApiError error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error.error());
    }

    public Collection<AclBinding> acls() {
        return acls;
    }

    public static DescribeAclsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeAclsResponse(ApiKeys.DESCRIBE_ACLS.responseSchema(version).read(buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }

    private void validate(short version) {
        if (version == 0) {
            final boolean unsupported = acls.stream()
                    .map(AclBinding::pattern)
                    .map(ResourcePattern::patternType)
                    .anyMatch(patternType -> patternType != PatternType.LITERAL);
            if (unsupported) {
                throw new UnsupportedVersionException("Version 0 only supports literal resource pattern types");
            }
        }

        final boolean unknown = acls.stream().anyMatch(AclBinding::isUnknown);
        if (unknown) {
            throw new IllegalArgumentException("Contain UNKNOWN elements");
        }
    }
}

