package com.jackniu.kafka.common.metrics;

public interface Stat  {
    /**
     * Record the given value
     * @param config The configuration to use for this metric
     * @param value The value to record
     * @param timeMs The POSIX time in milliseconds this value occurred
     */
    public void record(MetricConfig config, double value, long timeMs);

}
