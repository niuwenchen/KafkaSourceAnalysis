package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public abstract class AbstractRequestResponse {
    /**
     * Visible for testing.
     */
    /**
     * Visible for testing.
     */
    public static ByteBuffer serialize(Struct headerStruct, Struct bodyStruct) {
        ByteBuffer buffer = ByteBuffer.allocate(headerStruct.sizeOf() + bodyStruct.sizeOf());
        headerStruct.writeTo(buffer);
        bodyStruct.writeTo(buffer);
        buffer.rewind();
        return buffer;
    }

}
