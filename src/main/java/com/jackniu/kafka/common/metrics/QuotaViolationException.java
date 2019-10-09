package com.jackniu.kafka.common.metrics;

import com.jackniu.kafka.common.KafkaException;
import com.jackniu.kafka.common.MetricName;

public class QuotaViolationException  extends KafkaException {

    private static final long serialVersionUID = 1L;
    private final MetricName metricName;
    private final double value;
    private final double bound;

    public QuotaViolationException(MetricName metricName, double value, double bound) {
        super(String.format(
                "'%s' violated quota. Actual: %f, Threshold: %f",
                metricName,
                value,
                bound));
        this.metricName = metricName;
        this.value = value;
        this.bound = bound;
    }

    public MetricName metricName() {
        return metricName;
    }

    public double value() {
        return value;
    }

    public double bound() {
        return bound;
    }
}

