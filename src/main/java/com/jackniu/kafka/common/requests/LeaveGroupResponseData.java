package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.ApiMessage;
import com.jackniu.kafka.common.protocol.Writable;
import com.jackniu.kafka.common.protocol.Readable;
import com.jackniu.kafka.common.protocol.types.Field;
import com.jackniu.kafka.common.protocol.types.Schema;
import com.jackniu.kafka.common.protocol.types.Struct;
import com.jackniu.kafka.common.protocol.types.Type;

public class LeaveGroupResponseData implements ApiMessage {
    private int throttleTimeMs;
    private short errorCode;

    public static final Schema SCHEMA_0 =
            new Schema(
                    new Field("error_code", Type.INT16, "The error code, or 0 if there was no error.")
            );

    public static final Schema SCHEMA_1 =
            new Schema(
                    new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
                    new Field("error_code", Type.INT16, "The error code, or 0 if there was no error.")
            );

    public static final Schema SCHEMA_2 = SCHEMA_1;

    public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2
    };

    public LeaveGroupResponseData(Readable readable, short version) {
        read(readable, version);
    }

    public LeaveGroupResponseData(Struct struct, short version) {
        fromStruct(struct, version);
    }

    public LeaveGroupResponseData() {
        this.throttleTimeMs = 0;
        this.errorCode = (short) 0;
    }

    @Override
    public short apiKey() {
        return 13;
    }

    @Override
    public short lowestSupportedVersion() {
        return 0;
    }

    @Override
    public short highestSupportedVersion() {
        return 2;
    }

    @Override
    public void read(Readable readable, short version) {
        if (version >= 1) {
            this.throttleTimeMs = readable.readInt();
        } else {
            this.throttleTimeMs = 0;
        }
        this.errorCode = readable.readShort();
    }

    @Override
    public void write(Writable writable, short version) {
        if (version >= 1) {
            writable.writeInt(throttleTimeMs);
        }
        writable.writeShort(errorCode);
    }

    @Override
    public void fromStruct(Struct struct, short version) {
        if (version >= 1) {
            this.throttleTimeMs = struct.getInt("throttle_time_ms");
        } else {
            this.throttleTimeMs = 0;
        }
        this.errorCode = struct.getShort("error_code");
    }

    @Override
    public Struct toStruct(short version) {
        Struct struct = new Struct(SCHEMAS[version]);
        if (version >= 1) {
            struct.set("throttle_time_ms", this.throttleTimeMs);
        }
        struct.set("error_code", this.errorCode);
        return struct;
    }

    @Override
    public int size(short version) {
        int size = 0;
        if (version >= 1) {
            size += 4;
        }
        size += 2;
        return size;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LeaveGroupResponseData)) return false;
        LeaveGroupResponseData other = (LeaveGroupResponseData) obj;
        if (throttleTimeMs != other.throttleTimeMs) return false;
        if (errorCode != other.errorCode) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + throttleTimeMs;
        hashCode = 31 * hashCode + errorCode;
        return hashCode;
    }

    @Override
    public String toString() {
        return "LeaveGroupResponseData("
                + "throttleTimeMs=" + throttleTimeMs
                + ", errorCode=" + errorCode
                + ")";
    }

    public int throttleTimeMs() {
        return this.throttleTimeMs;
    }

    public short errorCode() {
        return this.errorCode;
    }

    public LeaveGroupResponseData setThrottleTimeMs(int v) {
        this.throttleTimeMs = v;
        return this;
    }

    public LeaveGroupResponseData setErrorCode(short v) {
        this.errorCode = v;
        return this;
    }
}

