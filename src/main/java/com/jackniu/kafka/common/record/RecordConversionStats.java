package com.jackniu.kafka.common.record;

public class RecordConversionStats {

    public static final RecordConversionStats EMPTY = new RecordConversionStats();

    private long temporaryMemoryBytes;
    private int numRecordsConverted;
    private long conversionTimeNanos;

    public RecordConversionStats(long temporaryMemoryBytes, int numRecordsConverted, long conversionTimeNanos) {
        this.temporaryMemoryBytes = temporaryMemoryBytes;
        this.numRecordsConverted = numRecordsConverted;
        this.conversionTimeNanos = conversionTimeNanos;
    }
    public RecordConversionStats() {
        this(0, 0, 0);
    }

    public void add(RecordConversionStats stats) {
        temporaryMemoryBytes += stats.temporaryMemoryBytes;
        numRecordsConverted += stats.numRecordsConverted;
        conversionTimeNanos += stats.conversionTimeNanos;
    }


    /**
     * Returns the number of temporary memory bytes allocated to process the records.
     * This size depends on whether the records need decompression and/or conversion:
     * <ul>
     *   <li>Non compressed, no conversion: zero</li>
     *   <li>Non compressed, with conversion: size of the converted buffer</li>
     *   <li>Compressed, no conversion: size of the original buffer after decompression</li>
     *   <li>Compressed, with conversion: size of the original buffer after decompression + size of the converted buffer uncompressed</li>
     * </ul>
     */
    public long temporaryMemoryBytes() {
        return temporaryMemoryBytes;
    }

    public int numRecordsConverted() {
        return numRecordsConverted;
    }

    public long conversionTimeNanos() {
        return conversionTimeNanos;
    }

    @Override
    public String toString() {
        return String.format("RecordConversionStats(temporaryMemoryBytes=%d, numRecordsConverted=%d, conversionTimeNanos=%d)",
                temporaryMemoryBytes, numRecordsConverted, conversionTimeNanos);
    }




}
