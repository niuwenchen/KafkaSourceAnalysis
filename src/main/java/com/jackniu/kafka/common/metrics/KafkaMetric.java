package com.jackniu.kafka.common.metrics;

import com.jackniu.kafka.common.Metric;
import com.jackniu.kafka.common.MetricName;
import com.jackniu.kafka.common.utils.Time;

public final  class KafkaMetric implements Metric {

    private MetricName metricName;
    private final Object lock;
    private final Time time;
    private final MetricValueProvider<?> metricValueProvider;
    private MetricConfig config;

    // public for testing
    public KafkaMetric(Object lock, MetricName metricName, MetricValueProvider<?> valueProvider,
                       MetricConfig config, Time time) {
        this.metricName = metricName;
        this.lock = lock;
        if (!(valueProvider instanceof Measurable) && !(valueProvider instanceof Gauge))
            throw new IllegalArgumentException("Unsupported metric value provider of class " + valueProvider.getClass());
        this.metricValueProvider = valueProvider;
        this.config = config;
        this.time = time;
    }

    public MetricConfig config() {
        return this.config;
    }

    @Override
    public MetricName metricName() {
        return this.metricName;
    }

    /**
     * See {@link Metric#value()} for the details on why this is deprecated.
     */
    @Override
    @Deprecated
    public double value() {
        return measurableValue(time.milliseconds());
    }

    @Override
    public Object metricValue() {
        long now = time.milliseconds();
        synchronized (this.lock) {
            if (this.metricValueProvider instanceof Measurable)
                return ((Measurable) metricValueProvider).measure(config, now);
            else if (this.metricValueProvider instanceof Gauge)
                return ((Gauge<?>) metricValueProvider).value(config, now);
            else
                throw new IllegalStateException("Not a valid metric: " + this.metricValueProvider.getClass());
        }
    }

    public Measurable measurable() {
        if (this.metricValueProvider instanceof Measurable)
            return (Measurable) metricValueProvider;
        else
            throw new IllegalStateException("Not a measurable: " + this.metricValueProvider.getClass());
    }

    double measurableValue(long timeMs) {
        synchronized (this.lock) {
            if (this.metricValueProvider instanceof Measurable)
                return ((Measurable) metricValueProvider).measure(config, timeMs);
            else
                return 0;
        }
    }

    public void config(MetricConfig config) {
        synchronized (lock) {
            this.config = config;
        }
    }
}

