package com.jackniu.kafka.common.record;

abstract class AbstractRecordBatch  implements RecordBatch {
    @Override
    public boolean hasProducerId() {
        return RecordBatch.NO_PRODUCER_ID < producerId();
    }

    @Override
    public long nextOffset() {
        return lastOffset() + 1;
    }

    @Override
    public boolean isCompressed() {
        return compressionType() != CompressionType.NONE;
    }
}
