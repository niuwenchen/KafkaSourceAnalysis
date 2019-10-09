package com.jackniu.kafka.common.metrics;

public interface Measurable extends MetricValueProvider<Double> {

    /**
     * Measure this quantity and return the result as a double
     * @param config The configuration for this metric
     * @param now The POSIX time in milliseconds the measurement is being taken
     * @return The measured value
     */
    double measure(MetricConfig config, long now);

}

