package com.jackniu.kafka.common.record;

import com.jackniu.kafka.common.KafkaException;
import com.jackniu.kafka.common.utils.AbstractIterator;

import java.io.IOException;

public class RecordBatchIterator<T extends RecordBatch> extends AbstractIterator<T> {

    private final LogInputStream<T> logInputStream;

    RecordBatchIterator(LogInputStream<T> logInputStream) {
        this.logInputStream = logInputStream;
    }

    @Override
    protected T makeNext() {
        try {
            T batch = logInputStream.nextBatch();
            if (batch == null)
                return allDone();
            return batch;
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }
}
