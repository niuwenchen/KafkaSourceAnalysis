package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.Node;
import com.jackniu.kafka.common.TopicPartition;
import com.jackniu.kafka.common.errors.UnsupportedVersionException;
import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.utils.CollectionUtils;
import com.jackniu.kafka.common.utils.Utils;
import java.nio.ByteBuffer;

import static com.jackniu.kafka.common.protocol.CommonFields.GROUP_ID;
import static com.jackniu.kafka.common.protocol.types.Type.INT8;
import static com.jackniu.kafka.common.protocol.types.Type.STRING;

public class FindCoordinatorRequest  extends AbstractRequest {
    private static final String COORDINATOR_KEY_KEY_NAME = "coordinator_key";
    private static final String COORDINATOR_TYPE_KEY_NAME = "coordinator_type";

    private static final Schema FIND_COORDINATOR_REQUEST_V0 = new Schema(GROUP_ID);

    private static final Schema FIND_COORDINATOR_REQUEST_V1 = new Schema(
            new Field("coordinator_key", STRING, "Id to use for finding the coordinator (for groups, this is the groupId, " +
                    "for transactional producers, this is the transactional id)"),
            new Field("coordinator_type", INT8, "The type of coordinator to find (0 = group, 1 = transaction)"));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema FIND_COORDINATOR_REQUEST_V2 = FIND_COORDINATOR_REQUEST_V1;

    public static Schema[] schemaVersions() {
        return new Schema[] {FIND_COORDINATOR_REQUEST_V0, FIND_COORDINATOR_REQUEST_V1, FIND_COORDINATOR_REQUEST_V2};
    }

    public static class Builder extends AbstractRequest.Builder<FindCoordinatorRequest> {
        private final String coordinatorKey;
        private final CoordinatorType coordinatorType;
        private final short minVersion;

        public Builder(CoordinatorType coordinatorType, String coordinatorKey) {
            super(ApiKeys.FIND_COORDINATOR);
            this.coordinatorType = coordinatorType;
            this.coordinatorKey = coordinatorKey;
            this.minVersion = coordinatorType == CoordinatorType.TRANSACTION ? (short) 1 : (short) 0;
        }

        @Override
        public FindCoordinatorRequest build(short version) {
            if (version < minVersion)
                throw new UnsupportedVersionException("Cannot create a v" + version + " FindCoordinator request " +
                        "because we require features supported only in " + minVersion + " or later.");
            return new FindCoordinatorRequest(coordinatorType, coordinatorKey, version);
        }

        public String coordinatorKey() {
            return coordinatorKey;
        }

        public CoordinatorType coordinatorType() {
            return coordinatorType;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=FindCoordinatorRequest, coordinatorKey=");
            bld.append(coordinatorKey);
            bld.append(", coordinatorType=");
            bld.append(coordinatorType);
            bld.append(")");
            return bld.toString();
        }
    }

    private final String coordinatorKey;
    private final CoordinatorType coordinatorType;

    private FindCoordinatorRequest(CoordinatorType coordinatorType, String coordinatorKey, short version) {
        super(ApiKeys.FIND_COORDINATOR, version);
        this.coordinatorType = coordinatorType;
        this.coordinatorKey = coordinatorKey;
    }

    public FindCoordinatorRequest(Struct struct, short version) {
        super(ApiKeys.FIND_COORDINATOR, version);

        if (struct.hasField(COORDINATOR_TYPE_KEY_NAME))
            this.coordinatorType = CoordinatorType.forId(struct.getByte(COORDINATOR_TYPE_KEY_NAME));
        else
            this.coordinatorType = CoordinatorType.GROUP;
        if (struct.hasField(GROUP_ID))
            this.coordinatorKey = struct.get(GROUP_ID);
        else
            this.coordinatorKey = struct.getString(COORDINATOR_KEY_KEY_NAME);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                return new FindCoordinatorResponse(Errors.forException(e), Node.noNode());
            case 1:
            case 2:
                return new FindCoordinatorResponse(throttleTimeMs, Errors.forException(e), Node.noNode());

            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.FIND_COORDINATOR.latestVersion()));
        }
    }

    public String coordinatorKey() {
        return coordinatorKey;
    }

    public CoordinatorType coordinatorType() {
        return coordinatorType;
    }

    public static FindCoordinatorRequest parse(ByteBuffer buffer, short version) {
        return new FindCoordinatorRequest(ApiKeys.FIND_COORDINATOR.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.FIND_COORDINATOR.requestSchema(version()));
        if (struct.hasField(GROUP_ID))
            struct.set(GROUP_ID, coordinatorKey);
        else
            struct.set(COORDINATOR_KEY_KEY_NAME, coordinatorKey);
        if (struct.hasField(COORDINATOR_TYPE_KEY_NAME))
            struct.set(COORDINATOR_TYPE_KEY_NAME, coordinatorType.id);
        return struct;
    }

    public enum CoordinatorType {
        GROUP((byte) 0), TRANSACTION((byte) 1);

        final byte id;

        CoordinatorType(byte id) {
            this.id = id;
        }

        public static CoordinatorType forId(byte id) {
            switch (id) {
                case 0:
                    return GROUP;
                case 1:
                    return TRANSACTION;
                default:
                    throw new IllegalArgumentException("Unknown coordinator type received: " + id);
            }
        }
    }

}
