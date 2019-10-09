package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.protocol.Errors;
import com.jackniu.kafka.common.protocol.types.ArrayOf;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.security.auth.KafkaPrincipal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.jackniu.kafka.common.protocol.CommonFields.PRINCIPAL_NAME;
import static com.jackniu.kafka.common.protocol.CommonFields.PRINCIPAL_TYPE;

public class DescribeDelegationTokenRequest extends AbstractRequest {
    private static final String OWNER_KEY_NAME = "owners";

    private final List<KafkaPrincipal> owners;

    public static final Schema TOKEN_DESCRIBE_REQUEST_V0 = new Schema(
            new Field(OWNER_KEY_NAME, ArrayOf.nullable(new Schema(PRINCIPAL_TYPE, PRINCIPAL_NAME)), "An array of token owners."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    public static final Schema TOKEN_DESCRIBE_REQUEST_V1 = TOKEN_DESCRIBE_REQUEST_V0;

    public static class Builder extends AbstractRequest.Builder<DescribeDelegationTokenRequest> {
        // describe token for the given list of owners, or null if we want to describe all tokens.
        private final List<KafkaPrincipal> owners;

        public Builder(List<KafkaPrincipal> owners) {
            super(ApiKeys.DESCRIBE_DELEGATION_TOKEN);
            this.owners = owners;
        }

        @Override
        public DescribeDelegationTokenRequest build(short version) {
            return new DescribeDelegationTokenRequest(version, owners);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type: DescribeDelegationTokenRequest").
                    append(", owners=").append(owners).
                    append(")");
            return bld.toString();
        }
    }

    private DescribeDelegationTokenRequest(short version, List<KafkaPrincipal> owners) {
        super(ApiKeys.DESCRIBE_DELEGATION_TOKEN, version);
        this.owners = owners;
    }

    public DescribeDelegationTokenRequest(Struct struct, short versionId) {
        super(ApiKeys.DESCRIBE_DELEGATION_TOKEN, versionId);

        Object[] ownerArray = struct.getArray(OWNER_KEY_NAME);

        if (ownerArray != null) {
            owners = new ArrayList<>();
            for (Object ownerObj : ownerArray) {
                Struct ownerObjStruct = (Struct) ownerObj;
                String principalType = ownerObjStruct.get(PRINCIPAL_TYPE);
                String principalName = ownerObjStruct.get(PRINCIPAL_NAME);
                owners.add(new KafkaPrincipal(principalType, principalName));
            }
        } else
            owners = null;
    }

    public static Schema[] schemaVersions() {
        return new Schema[]{TOKEN_DESCRIBE_REQUEST_V0, TOKEN_DESCRIBE_REQUEST_V1};
    }

    @Override
    protected Struct toStruct() {
        short version = version();
        Struct struct = new Struct(ApiKeys.DESCRIBE_DELEGATION_TOKEN.requestSchema(version));

        if (owners == null) {
            struct.set(OWNER_KEY_NAME, null);
        } else {
            Object[] ownersArray = new Object[owners.size()];

            int i = 0;
            for (KafkaPrincipal principal: owners) {
                Struct ownerStruct = struct.instance(OWNER_KEY_NAME);
                ownerStruct.set(PRINCIPAL_TYPE, principal.getPrincipalType());
                ownerStruct.set(PRINCIPAL_NAME, principal.getName());
                ownersArray[i++] = ownerStruct;
            }

            struct.set(OWNER_KEY_NAME, ownersArray);
        }

        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new DescribeDelegationTokenResponse(throttleTimeMs, Errors.forException(e));
    }

    public List<KafkaPrincipal> owners() {
        return owners;
    }

    public boolean ownersListEmpty() {
        return owners != null && owners.isEmpty();
    }

    public static DescribeDelegationTokenRequest parse(ByteBuffer buffer, short version) {
        return new DescribeDelegationTokenRequest(ApiKeys.DESCRIBE_DELEGATION_TOKEN.parseRequest(version, buffer), version);
    }
}

