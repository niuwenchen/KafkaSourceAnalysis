package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;

public class ApiVersionsRequest extends AbstractRequest {
    private static final Schema API_VERSIONS_REQUEST_V0 = new Schema();

    /* v1 request is the same as v0. Throttle time has been added to response */
    private static final Schema API_VERSIONS_REQUEST_V1 = API_VERSIONS_REQUEST_V0;

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema API_VERSIONS_REQUEST_V2 = API_VERSIONS_REQUEST_V1;

    public static Schema[] schemaVersions() {
        return new Schema[]{API_VERSIONS_REQUEST_V0, API_VERSIONS_REQUEST_V1, API_VERSIONS_REQUEST_V2};
    }

    public static class Builder extends AbstractRequest.Builder<ApiVersionsRequest> {

        public Builder() {
            super(ApiKeys.API_VERSIONS);
        }

        public Builder(short version) {
            super(ApiKeys.API_VERSIONS, version);
        }

        @Override
        public ApiVersionsRequest build(short version) {
            return new ApiVersionsRequest(version);
        }

        @Override
        public String toString() {
            return "(type=ApiVersionsRequest)";
        }
    }

    private final Short unsupportedRequestVersion;

    public ApiVersionsRequest(short version) {
        this(version, null);
    }

    public ApiVersionsRequest(short version, Short unsupportedRequestVersion) {
        super(ApiKeys.API_VERSIONS, version);

        // Unlike other request types, the broker handles ApiVersion requests with higher versions than
        // supported. It does so by treating the request as if it were v0 and returns a response using
        // the v0 response schema. The reason for this is that the client does not yet know what versions
        // a broker supports when this request is sent, so instead of assuming the lowest supported version,
        // it can use the most recent version and only fallback to the old version when necessary.
        this.unsupportedRequestVersion = unsupportedRequestVersion;
    }

    public ApiVersionsRequest(Struct struct, short version) {
        this(version, null);
    }

    public boolean hasUnsupportedRequestVersion() {
        return unsupportedRequestVersion != null;
    }

    @Override
    protected Struct toStruct() {
        return new Struct(ApiKeys.API_VERSIONS.requestSchema(version()));
    }

    @Override
    public ApiVersionsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short version = version();
        switch (version) {
            case 0:
                return new ApiVersionsResponse(Errors.forException(e), Collections.emptyList());
            case 1:
            case 2:
                return new ApiVersionsResponse(throttleTimeMs, Errors.forException(e), Collections.emptyList());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        version, this.getClass().getSimpleName(), ApiKeys.API_VERSIONS.latestVersion()));
        }
    }

    public static ApiVersionsRequest parse(ByteBuffer buffer, short version) {
        return new ApiVersionsRequest(ApiKeys.API_VERSIONS.parseRequest(version, buffer), version);
    }

}

