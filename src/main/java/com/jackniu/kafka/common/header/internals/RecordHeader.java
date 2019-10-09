package com.jackniu.kafka.common.header.internals;

import com.jackniu.kafka.common.header.Header;
import com.jackniu.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class RecordHeader implements Header {
    private final String key;
    private ByteBuffer valueBuffer;
    private byte[] value;

    public RecordHeader(String key, byte[] value) {
        Objects.requireNonNull(key, "Null header keys are not permitted");
        this.key = key;
        this.value = value;
    }

    public RecordHeader(String key, ByteBuffer valueBuffer) {
        Objects.requireNonNull(key, "Null header keys are not permitted");
        this.key = key;
        this.valueBuffer = valueBuffer;
    }

    public String key() {
        return key;
    }
    public byte[] value() {
        if (value == null && valueBuffer != null) {
            value = Utils.toArray(valueBuffer);
            valueBuffer = null;
        }
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        RecordHeader header = (RecordHeader) o;
        return (key == null ? header.key == null : key.equals(header.key)) &&
                Arrays.equals(value(), header.value());
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(value());
        return result;
    }

    @Override
    public String toString() {
        return "RecordHeader(key = " + key + ", value = " + Arrays.toString(value()) + ")";
    }




}
