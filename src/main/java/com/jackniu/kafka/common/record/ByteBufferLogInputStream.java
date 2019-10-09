package com.jackniu.kafka.common.record;

import com.jackniu.kafka.common.errors.CorruptRecordException;

import java.nio.ByteBuffer;

import static com.jackniu.kafka.common.record.Records.*;


public class ByteBufferLogInputStream  implements LogInputStream<MutableRecordBatch> {
    private final ByteBuffer buffer;
    private final int maxMessageSize;

    ByteBufferLogInputStream(ByteBuffer buffer, int maxMessageSize) {
        this.buffer = buffer;
        this.maxMessageSize = maxMessageSize;
    }

    public MutableRecordBatch nextBatch() {
        int remaining = buffer.remaining();

        Integer batchSize = nextBatchSize();
        if (batchSize == null || remaining < batchSize)
            return null;

        byte magic = buffer.get(buffer.position() + MAGIC_OFFSET);

        ByteBuffer batchSlice = buffer.slice();
        batchSlice.limit(batchSize);
        buffer.position(buffer.position() + batchSize);

        if (magic > RecordBatch.MAGIC_VALUE_V1)
            return new DefaultRecordBatch(batchSlice);
        else
            return new AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch(batchSlice);
    }

    /**
     * Validates the header of the next batch and returns batch size.
     * @return next batch size including LOG_OVERHEAD if buffer contains header up to
     *         magic byte, null otherwise
     * @throws CorruptRecordException if record size or magic is invalid
     */
    Integer nextBatchSize() throws CorruptRecordException {
        int remaining = buffer.remaining();
        if (remaining < LOG_OVERHEAD)
            return null;
        int recordSize = buffer.getInt(buffer.position() + SIZE_OFFSET);
        // V0 has the smallest overhead, stricter checking is done later
        if (recordSize < LegacyRecord.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Record size %d is less than the minimum record overhead (%d)",
                    recordSize, LegacyRecord.RECORD_OVERHEAD_V0));
        if (recordSize > maxMessageSize)
            throw new CorruptRecordException(String.format("Record size %d exceeds the largest allowable message size (%d).",
                    recordSize, maxMessageSize));

        if (remaining < HEADER_SIZE_UP_TO_MAGIC)
            return null;

        byte magic = buffer.get(buffer.position() + MAGIC_OFFSET);
        if (magic < 0 || magic > RecordBatch.CURRENT_MAGIC_VALUE)
            throw new CorruptRecordException("Invalid magic found in record: " + magic);

        return recordSize + LOG_OVERHEAD;
    }
}

