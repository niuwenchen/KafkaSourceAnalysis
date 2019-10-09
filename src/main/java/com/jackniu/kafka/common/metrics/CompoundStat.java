package com.jackniu.kafka.common.metrics;

import com.jackniu.kafka.common.MetricName;

import java.util.List;

public interface CompoundStat extends Stat {

    public List<NamedMeasurable> stats();

    public static class NamedMeasurable {

        private final MetricName name;
        private final Measurable stat;

        public NamedMeasurable(MetricName name, Measurable stat) {
            super();
            this.name = name;
            this.stat = stat;
        }

        public MetricName name() {
            return name;
        }

        public Measurable stat() {
            return stat;
        }

    }

}
