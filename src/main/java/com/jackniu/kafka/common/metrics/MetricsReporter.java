package com.jackniu.kafka.common.metrics;

import com.jackniu.kafka.common.Configurable;

import java.util.List;

public interface MetricsReporter extends Configurable, AutoCloseable {

    /**
     * This is called when the reporter is first registered to initially register all existing metrics
     * @param metrics All currently existing metrics
     */
    public void init(List<KafkaMetric> metrics);

    /**
     * This is called whenever a metric is updated or added
     * @param metric
     */
    public void metricChange(KafkaMetric metric);

    /**
     * This is called whenever a metric is removed
     * @param metric
     */
    public void metricRemoval(KafkaMetric metric);

    /**
     * Called when the metrics repository is closed.
     */
    public void close();

}

