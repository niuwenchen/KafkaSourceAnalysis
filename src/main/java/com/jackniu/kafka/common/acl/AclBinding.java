package com.jackniu.kafka.common.acl;

import com.jackniu.kafka.common.resource.PatternType;
import com.jackniu.kafka.common.resource.Resource;
import com.jackniu.kafka.common.resource.ResourcePattern;

import java.util.Objects;

public class AclBinding  {
    private final ResourcePattern pattern;
    private final AccessControlEntry entry;

    /**
     * Create an instance of this class with the provided parameters.
     *
     * @param pattern non-null resource pattern.
     * @param entry non-null entry
     */
    public AclBinding(ResourcePattern pattern, AccessControlEntry entry) {
        this.pattern = Objects.requireNonNull(pattern, "pattern");
        this.entry = Objects.requireNonNull(entry, "entry");
    }

    /**
     * Create an instance of this class with the provided parameters.
     *
     * @param resource non-null resource
     * @param entry non-null entry
     * @deprecated Since 2.0. Use {@link #AclBinding(ResourcePattern, AccessControlEntry)}
     */
    @Deprecated
    public AclBinding(Resource resource, AccessControlEntry entry) {
        this(new ResourcePattern(resource.resourceType(), resource.name(), PatternType.LITERAL), entry);
    }

    /**
     * @return true if this binding has any UNKNOWN components.
     */
    public boolean isUnknown() {
        return pattern.isUnknown() || entry.isUnknown();
    }

    /**
     * @return the resource pattern for this binding.
     */
    public ResourcePattern pattern() {
        return pattern;
    }

    /**
     * @return the access control entry for this binding.
     */
    public final AccessControlEntry entry() {
        return entry;
    }

    /**
     * Create a filter which matches only this AclBinding.
     */
    public AclBindingFilter toFilter() {
        return new AclBindingFilter(pattern.toFilter(), entry.toFilter());
    }

    @Override
    public String toString() {
        return "(pattern=" + pattern + ", entry=" + entry + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AclBinding that = (AclBinding) o;
        return Objects.equals(pattern, that.pattern) &&
                Objects.equals(entry, that.entry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern, entry);
    }
}
