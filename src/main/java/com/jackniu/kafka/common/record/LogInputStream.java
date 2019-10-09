package com.jackniu.kafka.common.record;

import java.io.IOException;

interface LogInputStream<T extends RecordBatch> {

    /**
     * Get the next record batch from the underlying input stream.
     *
     * @return The next record batch or null if there is none
     * @throws IOException for any IO errors
     */
    T nextBatch() throws IOException;
}
