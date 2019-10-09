package com.jackniu.kafka.common.record;

public class ConvertedRecords<T extends Records> {
    private final T records;
    private final RecordConversionStats recordConversionStats;

    public ConvertedRecords(T records, RecordConversionStats recordConversionStats) {
        this.records = records;
        this.recordConversionStats = recordConversionStats;
    }

    public T records() {
        return records;
    }

    public RecordConversionStats recordConversionStats() {
        return recordConversionStats;
    }

}
