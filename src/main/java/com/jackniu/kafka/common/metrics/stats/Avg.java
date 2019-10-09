package com.jackniu.kafka.common.metrics.stats;

import com.jackniu.kafka.common.metrics.MetricConfig;

import java.util.List;

public class Avg  extends SampledStat {

    public Avg() {
        super(0.0);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long now) {
        sample.value += value;
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        double total = 0.0;
        long count = 0;
        for (Sample s : samples) {
            total += s.value;
            count += s.eventCount;
        }
        return count == 0 ? Double.NaN : total / count;
    }

}


