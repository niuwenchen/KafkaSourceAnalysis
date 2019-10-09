package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.acl.AccessControlEntry;
import com.jackniu.kafka.common.acl.AclBinding;
import com.jackniu.kafka.common.errors.UnsupportedVersionException;
import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.types.ArrayOf;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.resource.PatternType;
import com.jackniu.kafka.common.resource.ResourcePattern;
import com.jackniu.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.jackniu.kafka.common.protocol.CommonFields.*;

public class CreateAclsRequest extends AbstractRequest {
    private final static String CREATIONS_KEY_NAME = "creations";

    private static final Schema CREATE_ACLS_REQUEST_V0 = new Schema(
            new Field(CREATIONS_KEY_NAME, new ArrayOf(new Schema(
                    RESOURCE_TYPE,
                    RESOURCE_NAME,
                    PRINCIPAL,
                    HOST,
                    OPERATION,
                    PERMISSION_TYPE))));

    /**
     * Version 1 adds RESOURCE_PATTERN_TYPE, to support more than just literal resource patterns.
     * For more info, see {@link PatternType}.
     *
     * Also, when the quota is violated, brokers will respond to a version 1 or later request before throttling.
     */
    private static final Schema CREATE_ACLS_REQUEST_V1 = new Schema(
            new Field(CREATIONS_KEY_NAME, new ArrayOf(new Schema(
                    RESOURCE_TYPE,
                    RESOURCE_NAME,
                    RESOURCE_PATTERN_TYPE,
                    PRINCIPAL,
                    HOST,
                    OPERATION,
                    PERMISSION_TYPE))));

    public static Schema[] schemaVersions() {
        return new Schema[]{CREATE_ACLS_REQUEST_V0, CREATE_ACLS_REQUEST_V1};
    }

    public static class AclCreation {
        private final AclBinding acl;

        public AclCreation(AclBinding acl) {
            this.acl = acl;
        }

        static AclCreation fromStruct(Struct struct) {
            ResourcePattern pattern = RequestUtils.resourcePatternromStructFields(struct);
            AccessControlEntry entry = RequestUtils.aceFromStructFields(struct);
            return new AclCreation(new AclBinding(pattern, entry));
        }

        public AclBinding acl() {
            return acl;
        }

        void setStructFields(Struct struct) {
            RequestUtils.resourcePatternSetStructFields(acl.pattern(), struct);
            RequestUtils.aceSetStructFields(acl.entry(), struct);
        }

        @Override
        public String toString() {
            return "(acl=" + acl + ")";
        }
    }

    public static class Builder extends AbstractRequest.Builder<CreateAclsRequest> {
        private final List<AclCreation> creations;

        public Builder(List<AclCreation> creations) {
            super(ApiKeys.CREATE_ACLS);
            this.creations = creations;
        }

        @Override
        public CreateAclsRequest build(short version) {
            return new CreateAclsRequest(version, creations);
        }

        @Override
        public String toString() {
            return "(type=CreateAclsRequest, creations=" + Utils.join(creations, ", ") + ")";
        }
    }

    private final List<AclCreation> aclCreations;

    CreateAclsRequest(short version, List<AclCreation> aclCreations) {
        super(ApiKeys.CREATE_ACLS, version);
        this.aclCreations = aclCreations;

        validate(aclCreations);
    }

    public CreateAclsRequest(Struct struct, short version) {
        super(ApiKeys.CREATE_ACLS, version);
        this.aclCreations = new ArrayList<>();
        for (Object creationStructObj : struct.getArray(CREATIONS_KEY_NAME)) {
            Struct creationStruct = (Struct) creationStructObj;
            aclCreations.add(AclCreation.fromStruct(creationStruct));
        }
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.CREATE_ACLS.requestSchema(version()));
        List<Struct> requests = new ArrayList<>();
        for (AclCreation creation : aclCreations) {
            Struct creationStruct = struct.instance(CREATIONS_KEY_NAME);
            creation.setStructFields(creationStruct);
            requests.add(creationStruct);
        }
        struct.set(CREATIONS_KEY_NAME, requests.toArray());
        return struct;
    }

    public List<AclCreation> aclCreations() {
        return aclCreations;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable throwable) {
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                List<CreateAclsResponse.AclCreationResponse> responses = new ArrayList<>();
                for (int i = 0; i < aclCreations.size(); i++)
                    responses.add(new CreateAclsResponse.AclCreationResponse(ApiError.fromThrowable(throwable)));
                return new CreateAclsResponse(throttleTimeMs, responses);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.CREATE_ACLS.latestVersion()));
        }
    }

    public static CreateAclsRequest parse(ByteBuffer buffer, short version) {
        return new CreateAclsRequest(ApiKeys.CREATE_ACLS.parseRequest(version, buffer), version);
    }

    private void validate(List<AclCreation> aclCreations) {
        if (version() == 0) {
            final boolean unsupported = aclCreations.stream()
                    .map(AclCreation::acl)
                    .map(AclBinding::pattern)
                    .map(ResourcePattern::patternType)
                    .anyMatch(patternType -> patternType != PatternType.LITERAL);
            if (unsupported) {
                throw new UnsupportedVersionException("Version 0 only supports literal resource pattern types");
            }
        }

        final boolean unknown = aclCreations.stream()
                .map(AclCreation::acl)
                .anyMatch(AclBinding::isUnknown);
        if (unknown) {
            throw new IllegalArgumentException("You can not create ACL bindings with unknown elements");
        }
    }
}
