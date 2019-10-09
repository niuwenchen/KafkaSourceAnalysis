package com.jackniu.kafka.common.record;

import com.jackniu.kafka.common.header.Header;
import com.jackniu.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class SimpleRecord  {
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final long timestamp;
    private final Header[] headers;

    public SimpleRecord(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        Objects.requireNonNull(headers, "Headers must be non-null");
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = headers;
    }

    public SimpleRecord(long timestamp, byte[] key, byte[] value, Header[] headers) {
        this(timestamp, Utils.wrapNullable(key), Utils.wrapNullable(value), headers);
    }

    public SimpleRecord(long timestamp, ByteBuffer key, ByteBuffer value) {
        this(timestamp, key, value, Record.EMPTY_HEADERS);
    }

    public SimpleRecord(long timestamp, byte[] key, byte[] value) {
        this(timestamp, Utils.wrapNullable(key), Utils.wrapNullable(value));
    }

    public SimpleRecord(long timestamp, byte[] value) {
        this(timestamp, null, value);
    }

    public SimpleRecord(byte[] value) {
        this(RecordBatch.NO_TIMESTAMP, null, value);
    }

    public SimpleRecord(byte[] key, byte[] value) {
        this(RecordBatch.NO_TIMESTAMP, key, value);
    }

    public SimpleRecord(Record record) {
        this(record.timestamp(), record.key(), record.value(), record.headers());
    }

    public ByteBuffer key() {
        return key;
    }

    public ByteBuffer value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }

    public Header[] headers() {
        return headers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SimpleRecord that = (SimpleRecord) o;
        return timestamp == that.timestamp &&
                (key == null ? that.key == null : key.equals(that.key)) &&
                (value == null ? that.value == null : value.equals(that.value)) &&
                Arrays.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + Arrays.hashCode(headers);
        return result;
    }

    @Override
    public String toString() {
        return String.format("SimpleRecord(timestamp=%d, key=%d bytes, value=%d bytes)",
                timestamp(),
                key == null ? 0 : key.limit(),
                value == null ? 0 : value.limit());
    }
}
