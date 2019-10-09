package com.jackniu.kafka.clients.producer.internal;

import static com.jackniu.kafka.common.record.RecordBatch.NO_PRODUCER_EPOCH;
import static com.jackniu.kafka.common.record.RecordBatch.NO_PRODUCER_ID;

public class ProducerIdAndEpoch  {
    static final ProducerIdAndEpoch NONE = new ProducerIdAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH);

    public final long producerId;
    public final short epoch;

    ProducerIdAndEpoch(long producerId, short epoch) {
        this.producerId = producerId;
        this.epoch = epoch;
    }

    public boolean isValid() {
        return NO_PRODUCER_ID < producerId;
    }

    @Override
    public String toString() {
        return "(producerId=" + producerId + ", epoch=" + epoch + ")";
    }

}
