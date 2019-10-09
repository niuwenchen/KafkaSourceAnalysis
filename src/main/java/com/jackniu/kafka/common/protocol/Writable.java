package com.jackniu.kafka.common.protocol;

import java.nio.charset.StandardCharsets;

public interface Writable {
    void writeByte(byte val);
    void writeShort(short val);
    void writeInt(int val);
    void writeLong(long val);
    void writeArray(byte[] arr);

    /**
     * Write a nullable byte array delimited by a four-byte length prefix.
     */
    default void writeNullableBytes(byte[] arr) {
        if (arr == null) {
            writeInt(-1);
        } else {
            writeBytes(arr);
        }
    }

    /**
     * Write a byte array delimited by a four-byte length prefix.
     */
    default void writeBytes(byte[] arr) {
        writeInt(arr.length);
        writeArray(arr);
    }

    /**
     * Write a nullable string delimited by a two-byte length prefix.
     */
    default void writeNullableString(String string) {
        if (string == null) {
            writeShort((short) -1);
        } else {
            writeString(string);
        }
    }

    /**
     * Write a string delimited by a two-byte length prefix.
     */
    default void writeString(String string) {
        byte[] arr = string.getBytes(StandardCharsets.UTF_8);
        if (arr.length > Short.MAX_VALUE) {
            throw new RuntimeException("Can't store string longer than " +
                    Short.MAX_VALUE);
        }
        writeShort((short) arr.length);
        writeArray(arr);
    }
}
