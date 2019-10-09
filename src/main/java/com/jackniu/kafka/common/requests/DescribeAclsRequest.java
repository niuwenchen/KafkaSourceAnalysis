package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.acl.AccessControlEntryFilter;
import com.jackniu.kafka.common.acl.AclBindingFilter;
import com.jackniu.kafka.common.errors.UnsupportedVersionException;
import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.resource.PatternType;
import com.jackniu.kafka.common.resource.ResourcePatternFilter;

import java.nio.ByteBuffer;
import java.util.Collections;

import static com.jackniu.kafka.common.protocol.CommonFields.*;

public class DescribeAclsRequest  extends AbstractRequest {
    private static final Schema DESCRIBE_ACLS_REQUEST_V0 = new Schema(
            RESOURCE_TYPE,
            RESOURCE_NAME_FILTER,
            PRINCIPAL_FILTER,
            HOST_FILTER,
            OPERATION,
            PERMISSION_TYPE);

    /**
     * V1 sees a new `RESOURCE_PATTERN_TYPE_FILTER` that controls how the filter handles different resource pattern types.
     * For more info, see {@link PatternType}.
     *
     * Also, when the quota is violated, brokers will respond to a version 1 or later request before throttling.
     */
    private static final Schema DESCRIBE_ACLS_REQUEST_V1 = new Schema(
            RESOURCE_TYPE,
            RESOURCE_NAME_FILTER,
            RESOURCE_PATTERN_TYPE_FILTER,
            PRINCIPAL_FILTER,
            HOST_FILTER,
            OPERATION,
            PERMISSION_TYPE);

    public static Schema[] schemaVersions() {
        return new Schema[]{DESCRIBE_ACLS_REQUEST_V0, DESCRIBE_ACLS_REQUEST_V1};
    }

    public static class Builder extends AbstractRequest.Builder<DescribeAclsRequest> {
        private final AclBindingFilter filter;

        public Builder(AclBindingFilter filter) {
            super(ApiKeys.DESCRIBE_ACLS);
            this.filter = filter;
        }

        @Override
        public DescribeAclsRequest build(short version) {
            return new DescribeAclsRequest(filter, version);
        }

        @Override
        public String toString() {
            return "(type=DescribeAclsRequest, filter=" + filter + ")";
        }
    }

    private final AclBindingFilter filter;

    DescribeAclsRequest(AclBindingFilter filter, short version) {
        super(ApiKeys.DELETE_ACLS, version);
        this.filter = filter;

        validate(filter, version);
    }

    public DescribeAclsRequest(Struct struct, short version) {
        super(ApiKeys.DELETE_ACLS, version);
        ResourcePatternFilter resourceFilter = RequestUtils.resourcePatternFilterFromStructFields(struct);
        AccessControlEntryFilter entryFilter = RequestUtils.aceFilterFromStructFields(struct);
        this.filter = new AclBindingFilter(resourceFilter, entryFilter);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.DESCRIBE_ACLS.requestSchema(version()));
        RequestUtils.resourcePatternFilterSetStructFields(filter.patternFilter(), struct);
        RequestUtils.aceFilterSetStructFields(filter.entryFilter(), struct);
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable throwable) {
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new DescribeAclsResponse(throttleTimeMs, ApiError.fromThrowable(throwable),
                        Collections.emptySet());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.DESCRIBE_ACLS.latestVersion()));
        }
    }

    public static DescribeAclsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeAclsRequest(ApiKeys.DESCRIBE_ACLS.parseRequest(version, buffer), version);
    }

    public AclBindingFilter filter() {
        return filter;
    }

    private void validate(AclBindingFilter filter, short version) {
        if (version == 0
                && filter.patternFilter().patternType() != PatternType.LITERAL
                && filter.patternFilter().patternType() != PatternType.ANY) {
            throw new UnsupportedVersionException("Version 0 only supports literal resource pattern types");
        }

        if (filter.isUnknown()) {
            throw new IllegalArgumentException("Filter contain UNKNOWN elements");
        }
    }
}
