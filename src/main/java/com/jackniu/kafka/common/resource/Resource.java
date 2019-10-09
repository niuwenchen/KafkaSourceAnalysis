package com.jackniu.kafka.common.resource;

import java.util.Objects;

public class Resource  {
    private final ResourceType resourceType;
    private final String name;

    /**
     * The name of the CLUSTER resource.
     */
    public final static String CLUSTER_NAME = "kafka-cluster";

    /**
     * A resource representing the whole cluster.
     */
    public final static Resource CLUSTER = new Resource(ResourceType.CLUSTER, CLUSTER_NAME);

    /**
     * Create an instance of this class with the provided parameters.
     *
     * @param resourceType non-null resource type
     * @param name non-null resource name
     */
    public Resource(ResourceType resourceType, String name) {
        Objects.requireNonNull(resourceType);
        this.resourceType = resourceType;
        Objects.requireNonNull(name);
        this.name = name;
    }

    /**
     * Return the resource type.
     */
    public ResourceType resourceType() {
        return resourceType;
    }

    /**
     * Return the resource name.
     */
    public String name() {
        return name;
    }

    /**
     * Create a filter which matches only this Resource.
     */
    public ResourceFilter toFilter() {
        return new ResourceFilter(resourceType, name);
    }

    @Override
    public String toString() {
        return "(resourceType=" + resourceType + ", name=" + ((name == null) ? "<any>" : name) + ")";
    }

    /**
     * Return true if this Resource has any UNKNOWN components.
     */
    public boolean isUnknown() {
        return resourceType.isUnknown();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Resource))
            return false;
        Resource other = (Resource) o;
        return resourceType.equals(other.resourceType) && Objects.equals(name, other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, name);
    }
}
