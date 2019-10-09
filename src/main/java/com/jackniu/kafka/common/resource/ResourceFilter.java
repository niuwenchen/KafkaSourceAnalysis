package com.jackniu.kafka.common.resource;


import java.util.Objects;

public class ResourceFilter  {

    private final ResourceType resourceType;
    private final String name;

    /**
     * Matches any resource.
     */
    public static final ResourceFilter ANY = new ResourceFilter(ResourceType.ANY, null);

    /**
     * Create an instance of this class with the provided parameters.
     *
     * @param resourceType non-null resource type
     * @param name resource name or null
     */
    public ResourceFilter(ResourceType resourceType, String name) {
        Objects.requireNonNull(resourceType);
        this.resourceType = resourceType;
        this.name = name;
    }

    /**
     * Return the resource type.
     */
    public ResourceType resourceType() {
        return resourceType;
    }

    /**
     * Return the resource name or null.
     */
    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "(resourceType=" + resourceType + ", name=" + ((name == null) ? "<any>" : name) + ")";
    }

    /**
     * Return true if this ResourceFilter has any UNKNOWN components.
     */
    public boolean isUnknown() {
        return resourceType.isUnknown();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ResourceFilter))
            return false;
        ResourceFilter other = (ResourceFilter) o;
        return resourceType.equals(other.resourceType) && Objects.equals(name, other.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceType, name);
    }

    /**
     * Return true if this filter matches the given Resource.
     */
    public boolean matches(Resource other) {
        if ((name != null) && (!name.equals(other.name())))
            return false;
        return (resourceType == ResourceType.ANY) || (resourceType.equals(other.resourceType()));
    }

    /**
     * Return true if this filter could only match one ACE. In other words, if there are no ANY or UNKNOWN fields.
     */
    public boolean matchesAtMostOne() {
        return findIndefiniteField() == null;
    }

    /**
     * Return a string describing an ANY or UNKNOWN field, or null if there is no such field.
     */
    public String findIndefiniteField() {
        if (resourceType == ResourceType.ANY)
            return "Resource type is ANY.";
        if (resourceType == ResourceType.UNKNOWN)
            return "Resource type is UNKNOWN.";
        if (name == null)
            return "Resource name is NULL.";
        return null;
    }
}
