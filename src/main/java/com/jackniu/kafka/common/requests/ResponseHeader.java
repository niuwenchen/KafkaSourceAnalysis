package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.types.BoundField;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static com.jackniu.kafka.common.protocol.types.Type.INT32;

public class ResponseHeader extends AbstractRequestResponse {
    public static final Schema SCHEMA = new Schema(
            new Field("correlation_id", INT32, "The user-supplied value passed in with the request"));
    private static final BoundField CORRELATION_KEY_FIELD = SCHEMA.get("correlation_id");

    private final int correlationId;

    public ResponseHeader(Struct struct) {
        correlationId = struct.getInt(CORRELATION_KEY_FIELD);
    }

    public ResponseHeader(int correlationId) {
        this.correlationId = correlationId;
    }

    public int sizeOf() {
        return toStruct().sizeOf();
    }

    public Struct toStruct() {
        Struct struct = new Struct(SCHEMA);
        struct.set(CORRELATION_KEY_FIELD, correlationId);
        return struct;
    }

    public int correlationId() {
        return correlationId;
    }

    public static ResponseHeader parse(ByteBuffer buffer) {
        return new ResponseHeader(SCHEMA.read(buffer));
    }

}

