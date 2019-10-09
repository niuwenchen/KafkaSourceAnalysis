package com.jackniu.kafka.common.record;

import com.jackniu.kafka.common.utils.ByteBufferOutputStream;

public interface MutableRecordBatch extends RecordBatch {
    /**
     * Set the last offset of this batch.
     * @param offset The last offset to use
     */
    void setLastOffset(long offset);

    /**
     * Set the max timestamp for this batch. When using log append time, this effectively overrides the individual
     * timestamps of all the records contained in the batch. To avoid recompression, the record fields are not updated
     * by this method, but clients ignore them if the timestamp time is log append time. Note that firstTimestamp is not
     * updated by this method.
     *
     * This typically requires re-computation of the batch's CRC.
     *
     * @param timestampType The timestamp type
     * @param maxTimestamp The maximum timestamp
     */
    void setMaxTimestamp(TimestampType timestampType, long maxTimestamp);

    /**
     * Set the partition leader epoch for this batch of records.
     * @param epoch The partition leader epoch to use
     */
    void setPartitionLeaderEpoch(int epoch);

    /**
     * Write this record batch into an output stream.
     * @param outputStream The buffer to write the batch to
     */
    void writeTo(ByteBufferOutputStream outputStream);

}
