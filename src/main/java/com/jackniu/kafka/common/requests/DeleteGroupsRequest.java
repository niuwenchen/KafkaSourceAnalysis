package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.ArrayOf;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.jackniu.kafka.common.protocol.types.Type.STRING;

public class DeleteGroupsRequest  extends AbstractRequest {
    private static final String GROUPS_KEY_NAME = "groups";

    /* DeleteGroups api */
    private static final Schema DELETE_GROUPS_REQUEST_V0 = new Schema(
            new Field(GROUPS_KEY_NAME, new ArrayOf(STRING), "An array of groups to be deleted."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema DELETE_GROUPS_REQUEST_V1 = DELETE_GROUPS_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{DELETE_GROUPS_REQUEST_V0, DELETE_GROUPS_REQUEST_V1};
    }

    private final Set<String> groups;

    public static class Builder extends AbstractRequest.Builder<DeleteGroupsRequest> {
        private final Set<String> groups;

        public Builder(Set<String> groups) {
            super(ApiKeys.DELETE_GROUPS);
            this.groups = groups;
        }

        @Override
        public DeleteGroupsRequest build(short version) {
            return new DeleteGroupsRequest(groups, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=DeleteGroupsRequest").
                    append(", groups=(").append(Utils.join(groups, ", ")).append(")").
                    append(")");
            return bld.toString();
        }
    }

    private DeleteGroupsRequest(Set<String> groups, short version) {
        super(ApiKeys.DELETE_GROUPS, version);
        this.groups = groups;
    }

    public DeleteGroupsRequest(Struct struct, short version) {
        super(ApiKeys.DELETE_GROUPS, version);
        Object[] groupsArray = struct.getArray(GROUPS_KEY_NAME);
        Set<String> groups = new HashSet<>(groupsArray.length);
        for (Object group : groupsArray)
            groups.add((String) group);

        this.groups = groups;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.DELETE_GROUPS.requestSchema(version()));
        struct.set(GROUPS_KEY_NAME, groups.toArray());
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        Map<String, Errors> groupErrors = new HashMap<>(groups.size());
        for (String group : groups)
            groupErrors.put(group, error);

        switch (version()) {
            case 0:
            case 1:
                return new DeleteGroupsResponse(throttleTimeMs, groupErrors);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        version(), ApiKeys.DELETE_GROUPS.name, ApiKeys.DELETE_GROUPS.latestVersion()));
        }
    }

    public Set<String> groups() {
        return groups;
    }

    public static DeleteGroupsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteGroupsRequest(ApiKeys.DELETE_GROUPS.parseRequest(version, buffer), version);
    }

}

