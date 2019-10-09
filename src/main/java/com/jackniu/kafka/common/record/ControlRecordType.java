package com.jackniu.kafka.common.record;

import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.protocol.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public enum ControlRecordType {
    ABORT((short) 0),
    COMMIT((short) 1),

    // UNKNOWN is used to indicate a control type which the client is not aware of and should be ignored
    UNKNOWN((short) -1);

    private static final Logger log = LoggerFactory.getLogger(ControlRecordType.class);

    static final short CURRENT_CONTROL_RECORD_KEY_VERSION = 0;
    static final int CURRENT_CONTROL_RECORD_KEY_SIZE = 4;
    private static final Schema CONTROL_RECORD_KEY_SCHEMA_VERSION_V0 = new Schema(
            new Field("version", Type.INT16),
            new Field("type", Type.INT16));

    final short type;

    ControlRecordType(short type) {
        this.type = type;
    }

    public Struct recordKey() {
        if (this == UNKNOWN)
            throw new IllegalArgumentException("Cannot serialize UNKNOWN control record type");

        Struct struct = new Struct(CONTROL_RECORD_KEY_SCHEMA_VERSION_V0);
        struct.set("version", CURRENT_CONTROL_RECORD_KEY_VERSION);
        struct.set("type", type);
        return struct;
    }

    public static short parseTypeId(ByteBuffer key) {
        if (key.remaining() < CURRENT_CONTROL_RECORD_KEY_SIZE)
            throw new InvalidRecordException("Invalid value size found for end control record key. Must have " +
                    "at least " + CURRENT_CONTROL_RECORD_KEY_SIZE + " bytes, but found only " + key.remaining());

        short version = key.getShort(0);
        if (version < 0)
            throw new InvalidRecordException("Invalid version found for control record: " + version +
                    ". May indicate data corruption");

        if (version != CURRENT_CONTROL_RECORD_KEY_VERSION)
            log.debug("Received unknown control record key version {}. Parsing as version {}", version,
                    CURRENT_CONTROL_RECORD_KEY_VERSION);
        return key.getShort(2);
    }

    public static ControlRecordType fromTypeId(short typeId) {
        switch (typeId) {
            case 0:
                return ABORT;
            case 1:
                return COMMIT;
            default:
                return UNKNOWN;
        }
    }

    public static ControlRecordType parse(ByteBuffer key) {
        return fromTypeId(parseTypeId(key));
    }
}
