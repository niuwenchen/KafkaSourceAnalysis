package com.jackniu.kafka.common.metrics.stats;

import com.jackniu.kafka.common.metrics.MetricConfig;

import java.util.List;

public class Sum  extends SampledStat {

    public Sum() {
        super(0);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long now) {
        sample.value += value;
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        double total = 0.0;
        for (Sample sample : samples)
            total += sample.value;
        return total;
    }

}

