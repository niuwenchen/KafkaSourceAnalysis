package com.jackniu.kafka.common;

import com.jackniu.kafka.common.utils.Utils;

import java.util.Map;

public final class MetricName {

    private final String name;
    private final String group;
    private final String description;
    private Map<String, String> tags;
    private int hash = 0;

    /**
     * Please create MetricName by method
     *
     * @param name        The name of the metric
     * @param group       logical group name of the metrics to which this metric belongs
     * @param description A human-readable description to include in the metric
     * @param tags        additional key/value attributes of the metric
     */
    public MetricName(String name, String group, String description, Map<String, String> tags) {
        this.name = Utils.notNull(name);
        this.group = Utils.notNull(group);
        this.description = Utils.notNull(description);
        this.tags = Utils.notNull(tags);
    }

    public String name() {
        return this.name;
    }

    public String group() {
        return this.group;
    }

    public Map<String, String> tags() {
        return this.tags;
    }

    public String description() {
        return this.description;
    }

    @Override
    public int hashCode() {
        if (hash != 0)
            return hash;
        final int prime = 31;
        int result = 1;
        result = prime * result + group.hashCode();
        result = prime * result + name.hashCode();
        result = prime * result + tags.hashCode();
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MetricName other = (MetricName) obj;
        return group.equals(other.group) && name.equals(other.name) && tags.equals(other.tags);
    }

    @Override
    public String toString() {
        return "MetricName [name=" + name + ", group=" + group + ", description="
                + description + ", tags=" + tags + "]";
    }
}
