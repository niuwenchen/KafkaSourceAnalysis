package com.jackniu.kafka.common;

public interface Metric  {
    /**
     * A name for this metric
     */
    MetricName metricName();

    /**
     * The value of the metric as double if the metric is measurable and `0.0` otherwise.
     *
     * @deprecated As of 1.0.0, use {@link #metricValue()} instead. This will be removed in a future major release.
     */
    @Deprecated
    double value();

    /**
     * The value of the metric, which may be measurable or a non-measurable gauge
     */
    Object metricValue();

}
