package com.jackniu.kafka.common.metrics.stats;

import com.jackniu.kafka.common.MetricName;

public class Percentile  {
    private final MetricName name;
    private final double percentile;

    public Percentile(MetricName name, double percentile) {
        super();
        this.name = name;
        this.percentile = percentile;
    }

    public MetricName name() {
        return this.name;
    }

    public double percentile() {
        return this.percentile;
    }

}
