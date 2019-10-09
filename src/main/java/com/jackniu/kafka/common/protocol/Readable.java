package com.jackniu.kafka.common.protocol;

import java.nio.charset.StandardCharsets;

public interface Readable {
    byte readByte();
    short readShort();
    int readInt();
    long readLong();
    void readArray(byte[] arr);

    /**
     * Read a Kafka-delimited string from a byte buffer.  The UTF-8 string
     * length is stored in a two-byte short.  If the length is negative, the
     * string is null.
     */
    default String readNullableString() {
        int length = readShort();
        if (length < 0) {
            return null;
        }
        byte[] arr = new byte[length];
        readArray(arr);
        return new String(arr, StandardCharsets.UTF_8);
    }

    /**
     * Read a Kafka-delimited array from a byte buffer.  The array length is
     * stored in a four-byte short.
     */
    default byte[] readNullableBytes() {
        int length = readInt();
        if (length < 0) {
            return null;
        }
        byte[] arr = new byte[length];
        readArray(arr);
        return arr;
    }
}
