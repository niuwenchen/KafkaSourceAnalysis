package com.jackniu.kafka.common.metrics.stats;

import com.jackniu.kafka.common.MetricName;
import com.jackniu.kafka.common.metrics.CompoundStat;
import com.jackniu.kafka.common.metrics.MetricConfig;
import com.jackniu.kafka.common.metrics.stats.Rate.SampledTotal;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Meter implements CompoundStat {

    private final MetricName rateMetricName;
    private final MetricName totalMetricName;
    private final Rate rate;
    private final Total total;

    /**
     * Construct a Meter with seconds as time unit and {@link SampledTotal} stats for Rate
     */
    public Meter(MetricName rateMetricName, MetricName totalMetricName) {
        this(TimeUnit.SECONDS, new SampledTotal(), rateMetricName, totalMetricName);
    }

    /**
     * Construct a Meter with provided time unit and {@link SampledTotal} stats for Rate
     */
    public Meter(TimeUnit unit, MetricName rateMetricName, MetricName totalMetricName) {
        this(unit, new SampledTotal(), rateMetricName, totalMetricName);
    }

    /**
     * Construct a Meter with seconds as time unit and provided {@link SampledStat} stats for Rate
     */
    public Meter(SampledStat rateStat, MetricName rateMetricName, MetricName totalMetricName) {
        this(TimeUnit.SECONDS, rateStat, rateMetricName, totalMetricName);
    }

    /**
     * Construct a Meter with provided time unit and provided {@link SampledStat} stats for Rate
     */
    public Meter(TimeUnit unit, SampledStat rateStat, MetricName rateMetricName, MetricName totalMetricName) {
        if (!(rateStat instanceof SampledTotal) && !(rateStat instanceof Count)) {
            throw new IllegalArgumentException("Meter is supported only for SampledTotal and Count");
        }
        this.total = new Total();
        this.rate = new Rate(unit, rateStat);
        this.rateMetricName = rateMetricName;
        this.totalMetricName = totalMetricName;
    }

    @Override
    public List<NamedMeasurable> stats() {
        return Arrays.asList(
                new NamedMeasurable(totalMetricName, total),
                new NamedMeasurable(rateMetricName, rate));
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        rate.record(config, value, timeMs);
        // Total metrics with Count stat should record 1.0 (as recorded in the count)
        double totalValue = (rate.stat instanceof Count) ? 1.0 : value;
        total.record(config, totalValue, timeMs);
    }
}

