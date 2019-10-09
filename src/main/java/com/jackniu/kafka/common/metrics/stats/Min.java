package com.jackniu.kafka.common.metrics.stats;

import com.jackniu.kafka.common.metrics.MetricConfig;

import java.util.List;

public class Min  extends SampledStat {

    public Min() {
        super(Double.MAX_VALUE);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long now) {
        sample.value = Math.min(sample.value, value);
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        double min = Double.MAX_VALUE;
        long count = 0;
        for (Sample sample : samples) {
            min = Math.min(min, sample.value);
            count += sample.eventCount;
        }
        return count == 0 ? Double.NaN : min;
    }

}
