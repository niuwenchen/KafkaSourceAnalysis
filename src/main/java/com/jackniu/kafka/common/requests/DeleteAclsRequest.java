package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.acl.AccessControlEntryFilter;
import com.jackniu.kafka.common.acl.AclBindingFilter;
import com.jackniu.kafka.common.errors.UnsupportedVersionException;
import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.types.ArrayOf;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.resource.PatternType;
import com.jackniu.kafka.common.resource.ResourcePatternFilter;
import com.jackniu.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.jackniu.kafka.common.protocol.ApiKeys.DELETE_ACLS;
import static com.jackniu.kafka.common.protocol.CommonFields.*;

public class DeleteAclsRequest extends AbstractRequest {
    private final static String FILTERS = "filters";

    private static final Schema DELETE_ACLS_REQUEST_V0 = new Schema(
            new Field(FILTERS, new ArrayOf(new Schema(
                    RESOURCE_TYPE,
                    RESOURCE_NAME_FILTER,
                    PRINCIPAL_FILTER,
                    HOST_FILTER,
                    OPERATION,
                    PERMISSION_TYPE))));

    /**
     * V1 sees a new `RESOURCE_PATTERN_TYPE_FILTER` that controls how the filter handles different resource pattern types.
     * For more info, see {@link PatternType}.
     *
     * Also, when the quota is violated, brokers will respond to a version 1 or later request before throttling.
     */
    private static final Schema DELETE_ACLS_REQUEST_V1 = new Schema(
            new Field(FILTERS, new ArrayOf(new Schema(
                    RESOURCE_TYPE,
                    RESOURCE_NAME_FILTER,
                    RESOURCE_PATTERN_TYPE_FILTER,
                    PRINCIPAL_FILTER,
                    HOST_FILTER,
                    OPERATION,
                    PERMISSION_TYPE))));

    public static Schema[] schemaVersions() {
        return new Schema[]{DELETE_ACLS_REQUEST_V0, DELETE_ACLS_REQUEST_V1};
    }

    public static class Builder extends AbstractRequest.Builder<DeleteAclsRequest> {
        private final List<AclBindingFilter> filters;

        public Builder(List<AclBindingFilter> filters) {
            super(DELETE_ACLS);
            this.filters = filters;
        }

        @Override
        public DeleteAclsRequest build(short version) {
            return new DeleteAclsRequest(version, filters);
        }

        @Override
        public String toString() {
            return "(type=DeleteAclsRequest, filters=" + Utils.join(filters, ", ") + ")";
        }
    }

    private final List<AclBindingFilter> filters;

    DeleteAclsRequest(short version, List<AclBindingFilter> filters) {
        super(ApiKeys.DELETE_ACLS, version);
        this.filters = filters;

        validate(version, filters);
    }

    public DeleteAclsRequest(Struct struct, short version) {
        super(ApiKeys.DELETE_ACLS, version);
        this.filters = new ArrayList<>();
        for (Object filterStructObj : struct.getArray(FILTERS)) {
            Struct filterStruct = (Struct) filterStructObj;
            ResourcePatternFilter resourceFilter = RequestUtils.resourcePatternFilterFromStructFields(filterStruct);
            AccessControlEntryFilter aceFilter = RequestUtils.aceFilterFromStructFields(filterStruct);
            this.filters.add(new AclBindingFilter(resourceFilter, aceFilter));
        }
    }

    public List<AclBindingFilter> filters() {
        return filters;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(DELETE_ACLS.requestSchema(version()));
        List<Struct> filterStructs = new ArrayList<>();
        for (AclBindingFilter filter : filters) {
            Struct filterStruct = struct.instance(FILTERS);
            RequestUtils.resourcePatternFilterSetStructFields(filter.patternFilter(), filterStruct);
            RequestUtils.aceFilterSetStructFields(filter.entryFilter(), filterStruct);
            filterStructs.add(filterStruct);
        }
        struct.set(FILTERS, filterStructs.toArray());
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable throwable) {
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                List<DeleteAclsResponse.AclFilterResponse> responses = new ArrayList<>();
                for (int i = 0; i < filters.size(); i++) {
                    responses.add(new DeleteAclsResponse.AclFilterResponse(
                            ApiError.fromThrowable(throwable), Collections.emptySet()));
                }
                return new DeleteAclsResponse(throttleTimeMs, responses);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.DELETE_ACLS.latestVersion()));
        }
    }

    public static DeleteAclsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteAclsRequest(DELETE_ACLS.parseRequest(version, buffer), version);
    }

    private void validate(short version, List<AclBindingFilter> filters) {
        if (version == 0) {
            final boolean unsupported = filters.stream()
                    .map(AclBindingFilter::patternFilter)
                    .map(ResourcePatternFilter::patternType)
                    .anyMatch(patternType -> patternType != PatternType.LITERAL && patternType != PatternType.ANY);
            if (unsupported) {
                throw new UnsupportedVersionException("Version 0 only supports literal resource pattern types");
            }
        }

        final boolean unknown = filters.stream().anyMatch(AclBindingFilter::isUnknown);
        if (unknown) {
            throw new IllegalArgumentException("Filters contain UNKNOWN elements");
        }
    }
}

