package com.jackniu.kafka.common.metrics.stats;

import com.jackniu.kafka.common.metrics.MeasurableStat;
import com.jackniu.kafka.common.metrics.MetricConfig;

public class Total  implements MeasurableStat {

    private double total;

    public Total() {
        this.total = 0.0;
    }

    public Total(double value) {
        this.total = value;
    }

    @Override
    public void record(MetricConfig config, double value, long now) {
        this.total += value;
    }

    @Override
    public double measure(MetricConfig config, long now) {
        return this.total;
    }

}

