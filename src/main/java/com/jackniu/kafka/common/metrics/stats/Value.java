package com.jackniu.kafka.common.metrics.stats;

import com.jackniu.kafka.common.metrics.MeasurableStat;
import com.jackniu.kafka.common.metrics.MetricConfig;

public class Value implements MeasurableStat {
    private double value = 0;

    @Override
    public double measure(MetricConfig config, long now) {
        return value;
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        this.value = value;
    }
}


