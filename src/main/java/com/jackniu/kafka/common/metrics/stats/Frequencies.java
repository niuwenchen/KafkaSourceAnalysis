package com.jackniu.kafka.common.metrics.stats;

import com.jackniu.kafka.common.MetricName;
import com.jackniu.kafka.common.metrics.CompoundStat;
import com.jackniu.kafka.common.metrics.Measurable;
import com.jackniu.kafka.common.metrics.MetricConfig;

import java.util.ArrayList;
import java.util.List;

public class Frequencies  extends SampledStat implements CompoundStat {

    /**
     * Create a Frequencies instance with metrics for the frequency of a boolean sensor that records 0.0 for
     * false and 1.0 for true.
     *
     * @param falseMetricName the name of the metric capturing the frequency of failures; may be null if not needed
     * @param trueMetricName  the name of the metric capturing the frequency of successes; may be null if not needed
     * @return the Frequencies instance; never null
     * @throws IllegalArgumentException if both {@code falseMetricName} and {@code trueMetricName} are null
     */
    public static Frequencies forBooleanValues(MetricName falseMetricName, MetricName trueMetricName) {
        List<Frequency> frequencies = new ArrayList<>();
        if (falseMetricName != null) {
            frequencies.add(new Frequency(falseMetricName, 0.0));
        }
        if (trueMetricName != null) {
            frequencies.add(new Frequency(trueMetricName, 1.0));
        }
        if (frequencies.isEmpty()) {
            throw new IllegalArgumentException("Must specify at least one metric name");
        }
        Frequency[] frequencyArray = frequencies.toArray(new Frequency[frequencies.size()]);
        return new Frequencies(2, 0.0, 1.0, frequencyArray);
    }

    private final Frequency[] frequencies;
    private final Histogram.BinScheme binScheme;

    /**
     * Create a Frequencies that captures the values in the specified range into the given number of buckets,
     * where the buckets are centered around the minimum, maximum, and intermediate values.
     *
     * @param buckets     the number of buckets; must be at least 1
     * @param min         the minimum value to be captured
     * @param max         the maximum value to be captured
     * @param frequencies the list of {@link Frequency} metrics, which at most should be one per bucket centered
     *                    on the bucket's value, though not every bucket need to correspond to a metric if the
     *                    value is not needed
     * @throws IllegalArgumentException if any of the {@link Frequency} objects do not have a
     *                                  {@link Frequency#centerValue() center value} within the specified range
     */
    public Frequencies(int buckets, double min, double max, Frequency... frequencies) {
        super(0.0); // initial value is unused by this implementation
        if (max < min) {
            throw new IllegalArgumentException("The maximum value " + max
                    + " must be greater than the minimum value " + min);
        }
        if (buckets < 1) {
            throw new IllegalArgumentException("Must be at least 1 bucket");
        }
        if (buckets < frequencies.length) {
            throw new IllegalArgumentException("More frequencies than buckets");
        }
        this.frequencies = frequencies;
        for (Frequency freq : frequencies) {
            if (min > freq.centerValue() || max < freq.centerValue()) {
                throw new IllegalArgumentException("The frequency centered at '" + freq.centerValue()
                        + "' is not within the range [" + min + "," + max + "]");
            }
        }
        double halfBucketWidth = (max - min) / (buckets - 1) / 2.0;
        this.binScheme = new Histogram.ConstantBinScheme(buckets, min - halfBucketWidth, max + halfBucketWidth);
    }

    @Override
    public List<CompoundStat.NamedMeasurable> stats() {
        List<NamedMeasurable> ms = new ArrayList<>(frequencies.length);
        for (Frequency frequency : frequencies) {
            final double center = frequency.centerValue();
            ms.add(new NamedMeasurable(frequency.name(), new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return frequency(config, now, center);
                }
            }));
        }
        return ms;
    }

    /**
     * Return the computed frequency describing the number of occurrences of the values in the bucket for the given
     * center point, relative to the total number of occurrences in the samples.
     *
     * @param config      the metric configuration
     * @param now         the current time in milliseconds
     * @param centerValue the value corresponding to the center point of the bucket
     * @return the frequency of the values in the bucket relative to the total number of samples
     */
    public double frequency(MetricConfig config, long now, double centerValue) {
        purgeObsoleteSamples(config, now);
        long totalCount = 0;
        for (Sample sample : samples) {
            totalCount += sample.eventCount;
        }
        if (totalCount == 0) {
            return 0.0d;
        }
        // Add up all of the counts in the bin corresponding to the center value
        float count = 0.0f;
        int binNum = binScheme.toBin(centerValue);
        for (Sample s : samples) {
            HistogramSample sample = (HistogramSample) s;
            float[] hist = sample.histogram.counts();
            count += hist[binNum];
        }
        // Compute the ratio of counts to total counts
        return count / (double) totalCount;
    }

    double totalCount() {
        long count = 0;
        for (Sample sample : samples) {
            count += sample.eventCount;
        }
        return count;
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        return totalCount();
    }

    @Override
    protected HistogramSample newSample(long timeMs) {
        return new HistogramSample(binScheme, timeMs);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long timeMs) {
        HistogramSample hist = (HistogramSample) sample;
        hist.histogram.record(value);
    }

    private static class HistogramSample extends SampledStat.Sample {

        private final Histogram histogram;

        private HistogramSample(Histogram.BinScheme scheme, long now) {
            super(0.0, now);
            histogram = new Histogram(scheme);
        }

        @Override
        public void reset(long now) {
            super.reset(now);
            histogram.clear();
        }
    }
}
