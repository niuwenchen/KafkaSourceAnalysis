package com.jackniu.kafka.common.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricConfig  {
    private Quota quota;
    private int samples;
    private long eventWindow;
    private long timeWindowMs;
    private Map<String, String> tags;
    private Sensor.RecordingLevel recordingLevel;

    public MetricConfig() {
        super();
        this.quota = null;
        this.samples = 2;
        this.eventWindow = Long.MAX_VALUE;
        this.timeWindowMs = TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);
        this.tags = new LinkedHashMap<>();
        this.recordingLevel = Sensor.RecordingLevel.INFO;
    }

    public Quota quota() {
        return this.quota;
    }

    public MetricConfig quota(Quota quota) {
        this.quota = quota;
        return this;
    }

    public long eventWindow() {
        return eventWindow;
    }

    public MetricConfig eventWindow(long window) {
        this.eventWindow = window;
        return this;
    }

    public long timeWindowMs() {
        return timeWindowMs;
    }

    public MetricConfig timeWindow(long window, TimeUnit unit) {
        this.timeWindowMs = TimeUnit.MILLISECONDS.convert(window, unit);
        return this;
    }

    public Map<String, String> tags() {
        return this.tags;
    }

    public MetricConfig tags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }

    public int samples() {
        return this.samples;
    }

    public MetricConfig samples(int samples) {
        if (samples < 1)
            throw new IllegalArgumentException("The number of samples must be at least 1.");
        this.samples = samples;
        return this;
    }

    public Sensor.RecordingLevel recordLevel() {
        return this.recordingLevel;
    }

    public MetricConfig recordLevel(Sensor.RecordingLevel recordingLevel) {
        this.recordingLevel = recordingLevel;
        return this;
    }


    @Override
    public String toString() {

        return "{"
                + "quote: "+ this.quota
                + ",samples: " + this.samples
                + ",eventWindow: "+ this.eventWindow
                + ",timeWindowMs: "+ this.timeWindowMs
                + ",tags: "+ this.tags
                + ",recordingLevel: " + this.recordingLevel
                +"}";
    }
}
