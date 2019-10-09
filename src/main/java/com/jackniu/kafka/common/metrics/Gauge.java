package com.jackniu.kafka.common.metrics;

public interface Gauge<T> extends MetricValueProvider<T> {

    /**
     * Returns the current value associated with this gauge.
     * @param config The configuration for this metric
     * @param now The POSIX time in milliseconds the measurement is being taken
     */
    T value(MetricConfig config, long now);

}
