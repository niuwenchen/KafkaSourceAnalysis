package com.jackniu.kafka.common.requests;

import com.jackniu.kafka.common.protocol.Errors;

import java.util.Objects;

import static com.jackniu.kafka.common.record.RecordBatch.NO_PARTITION_LEADER_EPOCH;

public class EpochEndOffset {
    public static final long UNDEFINED_EPOCH_OFFSET = NO_PARTITION_LEADER_EPOCH;
    public static final int UNDEFINED_EPOCH = NO_PARTITION_LEADER_EPOCH;

    private Errors error;
    private int leaderEpoch;  // introduced in V1
    private long endOffset;

    public EpochEndOffset(Errors error, int leaderEpoch, long endOffset) {
        this.error = error;
        this.leaderEpoch = leaderEpoch;
        this.endOffset = endOffset;
    }

    public EpochEndOffset(int leaderEpoch, long endOffset) {
        this.error = Errors.NONE;
        this.leaderEpoch = leaderEpoch;
        this.endOffset = endOffset;
    }

    public Errors error() {
        return error;
    }

    public boolean hasError() {
        return error != Errors.NONE;
    }

    public long endOffset() {
        return endOffset;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    @Override
    public String toString() {
        return "EpochEndOffset{" +
                "error=" + error +
                ", leaderEpoch=" + leaderEpoch +
                ", endOffset=" + endOffset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EpochEndOffset that = (EpochEndOffset) o;

        return Objects.equals(error, that.error)
                && Objects.equals(leaderEpoch, that.leaderEpoch)
                && Objects.equals(endOffset, that.endOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(error, leaderEpoch, endOffset);
    }
}

