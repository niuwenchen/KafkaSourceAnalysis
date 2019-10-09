package com.jackniu.kafka.common.record;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

public class DefaultRecordsSend extends RecordsSend<Records> {
    public DefaultRecordsSend(String destination, Records records) {
        this(destination, records, records.sizeInBytes());
    }

    public DefaultRecordsSend(String destination, Records records, int maxBytesToWrite) {
        super(destination, records, maxBytesToWrite);
    }

    @Override
    protected long writeTo(GatheringByteChannel channel, long previouslyWritten, int remaining) throws IOException {
        return records().writeTo(channel, previouslyWritten, remaining);
    }

}
