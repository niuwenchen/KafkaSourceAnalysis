package com.jackniu.kafka.clients.producer.internal;

import com.jackniu.kafka.common.MetricNameTemplate;
import com.jackniu.kafka.common.metrics.MetricConfig;
import com.jackniu.kafka.common.metrics.Metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ProducerMetrics  {
    public final SenderMetricsRegistry senderMetrics;
    private final Metrics metrics;

    public ProducerMetrics(Metrics metrics) {
        this.metrics = metrics;
        this.senderMetrics = new SenderMetricsRegistry(this.metrics);
    }

    private List<MetricNameTemplate> getAllTemplates() {
        List<MetricNameTemplate> l = new ArrayList<>(this.senderMetrics.allTemplates());
        return l;
    }

    public static void main(String[] args) {
        Map<String, String> metricTags = Collections.singletonMap("client-id", "client-id");
        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        Metrics metrics = new Metrics(metricConfig);

        ProducerMetrics metricsRegistry = new ProducerMetrics(metrics);
        System.out.println(Metrics.toHtmlTable("kafka.producer", metricsRegistry.getAllTemplates()));
    }
}
