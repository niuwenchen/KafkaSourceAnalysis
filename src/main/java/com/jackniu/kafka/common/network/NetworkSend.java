package com.jackniu.kafka.common.network;

import java.nio.ByteBuffer;

public class NetworkSend extends ByteBufferSend {

    public NetworkSend(String destination, ByteBuffer buffer) {
        super(destination, sizeDelimit(buffer));
    }

    private static ByteBuffer[] sizeDelimit(ByteBuffer buffer) {
        return new ByteBuffer[] {sizeBuffer(buffer.remaining()), buffer};
    }

    private static ByteBuffer sizeBuffer(int size) {
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(size);
        sizeBuffer.rewind();
        return sizeBuffer;
    }

}

