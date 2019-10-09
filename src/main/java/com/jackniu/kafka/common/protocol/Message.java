package com.jackniu.kafka.common.protocol;

import com.jackniu.kafka.common.protocol.types.Struct;

public interface Message {
    /**
     * Returns the lowest supported API key of this message, inclusive.
     */
    short lowestSupportedVersion();

    /**
     * Returns the highest supported API key of this message, inclusive.
     */
    short highestSupportedVersion();

    /**
     * Returns the number of bytes it would take to write out this message.
     *
     * @param version       The version to use.
     *
     * @throws {@see org.apache.kafka.common.errors.UnsupportedVersionException}
     *                      If the specified version is too new to be supported
     *                      by this software.
     */
    int size(short version);

    /**
     * Writes out this message to the given ByteBuffer.
     *
     * @param writable      The destination writable.
     * @param version       The version to use.
     *
     * @throws {@see org.apache.kafka.common.errors.UnsupportedVersionException}
     *                      If the specified version is too new to be supported
     *                      by this software.
     */
    void write(Writable writable, short version);

    /**
     * Reads this message from the given ByteBuffer.  This will overwrite all
     * relevant fields with information from the byte buffer.
     *
     * @param readable      The source readable.
     * @param version       The version to use.
     *
     * @throws {@see org.apache.kafka.common.errors.UnsupportedVersionException}
     *                      If the specified version is too new to be supported
     *                      by this software.
     */
    void read(Readable readable, short version);

    /**
     * Reads this message from the a Struct object.  This will overwrite all
     * relevant fields with information from the Struct.
     *
     * @param struct        The source struct.
     * @param version       The version to use.
     */
    void fromStruct(Struct struct, short version);

    /**
     * Writes out this message to a Struct.
     *
     * @param version       The version to use.
     *
     * @throws {@see org.apache.kafka.common.errors.UnsupportedVersionException}
     *                      If the specified version is too new to be supported
     *                      by this software.
     */
    Struct toStruct(short version);

}
