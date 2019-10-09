package com.jackniu.kafka.common.record;

import com.jackniu.kafka.common.network.Send;
import com.jackniu.kafka.common.network.TransportLayers;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public  abstract  class RecordsSend<T extends BaseRecords> implements Send {
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

    private final String destination;
    private final T records;
    private final int maxBytesToWrite;
    private int remaining;
    private boolean pending = false;

    protected RecordsSend(String destination, T records, int maxBytesToWrite) {
        this.destination = destination;
        this.records = records;
        this.maxBytesToWrite = maxBytesToWrite;
        this.remaining = maxBytesToWrite;
    }

    @Override
    public String destination() {
        return destination;
    }

    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    @Override
    public final long writeTo(GatheringByteChannel channel) throws IOException {
        long written = 0;

        if (remaining > 0) {
            written = writeTo(channel, size() - remaining, remaining);
            if (written < 0)
                throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
            remaining -= written;
        }

        pending = TransportLayers.hasPendingWrites(channel);
        if (remaining <= 0 && pending)
            channel.write(EMPTY_BYTE_BUFFER);

        return written;
    }

    @Override
    public long size() {
        return maxBytesToWrite;
    }

    protected T records() {
        return records;
    }

/**
 * Write records up to `remaining` bytes to `channel`. The implementation is allowed to be stateful. The contract
 * from the caller is that the first invocation will be with `previouslyWritten` equal to 0, and `remaining` equal to
 * the to maximum bytes we want to write the to `channel`. `previouslyWritten` and `remaining` will be adjusted
 * appropriately for every subsequent invocation. See {@link #writeTo} for example expected usage.
  **/
    protected abstract long writeTo(GatheringByteChannel channel, long previouslyWritten, int remaining) throws IOException;

}
