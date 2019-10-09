package com.jackniu.kafka.common.metrics.stats;

import com.jackniu.kafka.common.MetricName;

public class Frequency  {
    private final MetricName name;
    private final double centerValue;

    /**
     * Create an instance with the given name and center point value.
     *
     * @param name        the name of the frequency metric; may not be null
     * @param centerValue the value identifying the {@link Frequencies} bucket to be reported
     */
    public Frequency(MetricName name, double centerValue) {
        this.name = name;
        this.centerValue = centerValue;
    }

    /**
     * Get the name of this metric.
     *
     * @return the metric name; never null
     */
    public MetricName name() {
        return this.name;
    }

    /**
     * Get the value of this metrics center point.
     *
     * @return the center point value
     */
    public double centerValue() {
        return this.centerValue;
    }

}
