package com.jackniu.kafka.common.metrics.stats;

import com.jackniu.kafka.common.metrics.MetricConfig;

import java.util.List;

public final class Max extends SampledStat {

    public Max() {
        super(Double.NEGATIVE_INFINITY);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long now) {
        sample.value = Math.max(sample.value, value);
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        double max = Double.NEGATIVE_INFINITY;
        long count = 0;
        for (Sample sample : samples) {
            max = Math.max(max, sample.value);
            count += sample.eventCount;
        }
        return count == 0 ? Double.NaN : max;
    }

}
