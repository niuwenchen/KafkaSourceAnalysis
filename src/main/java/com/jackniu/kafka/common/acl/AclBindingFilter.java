package com.jackniu.kafka.common.acl;

import com.jackniu.kafka.common.resource.PatternType;
import com.jackniu.kafka.common.resource.ResourceFilter;
import com.jackniu.kafka.common.resource.ResourcePatternFilter;

import java.util.Objects;

public class AclBindingFilter  {
    private final ResourcePatternFilter patternFilter;
    private final AccessControlEntryFilter entryFilter;

    /**
     * A filter which matches any ACL binding.
     */
    public static final AclBindingFilter ANY = new AclBindingFilter(ResourcePatternFilter.ANY, AccessControlEntryFilter.ANY);

    /**
     * Create an instance of this filter with the provided parameters.
     *
     * @param patternFilter non-null pattern filter
     * @param entryFilter non-null access control entry filter
     */
    public AclBindingFilter(ResourcePatternFilter patternFilter, AccessControlEntryFilter entryFilter) {
        this.patternFilter = Objects.requireNonNull(patternFilter, "patternFilter");
        this.entryFilter = Objects.requireNonNull(entryFilter, "entryFilter");
    }

    /**
     * Create an instance of this filter with the provided parameters.
     *
     * @param resourceFilter non-null resource filter
     * @param entryFilter non-null access control entry filter
     * @deprecated Since 2.0. Use {@link #AclBindingFilter(ResourcePatternFilter, AccessControlEntryFilter)}
     */
    @Deprecated
    public AclBindingFilter(ResourceFilter resourceFilter, AccessControlEntryFilter entryFilter) {
        this(new ResourcePatternFilter(resourceFilter.resourceType(), resourceFilter.name(), PatternType.LITERAL), entryFilter);
    }

    /**
     * @return {@code true} if this filter has any UNKNOWN components.
     */
    public boolean isUnknown() {
        return patternFilter.isUnknown() || entryFilter.isUnknown();
    }

    /**
     * @return the resource pattern filter.
     */
    public ResourcePatternFilter patternFilter() {
        return patternFilter;
    }

    /**
     * @return the access control entry filter.
     */
    public final AccessControlEntryFilter entryFilter() {
        return entryFilter;
    }

    @Override
    public String toString() {
        return "(patternFilter=" + patternFilter + ", entryFilter=" + entryFilter + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AclBindingFilter that = (AclBindingFilter) o;
        return Objects.equals(patternFilter, that.patternFilter) &&
                Objects.equals(entryFilter, that.entryFilter);
    }

    /**
     * Return true if the resource and entry filters can only match one ACE. In other words, if
     * there are no ANY or UNKNOWN fields.
     */
    public boolean matchesAtMostOne() {
        return patternFilter.matchesAtMostOne() && entryFilter.matchesAtMostOne();
    }

    /**
     * Return a string describing an ANY or UNKNOWN field, or null if there is no such field.
     */
    public String findIndefiniteField() {
        String indefinite = patternFilter.findIndefiniteField();
        if (indefinite != null)
            return indefinite;
        return entryFilter.findIndefiniteField();
    }

    /**
     * Return true if the resource filter matches the binding's resource and the entry filter matches binding's entry.
     */
    public boolean matches(AclBinding binding) {
        return patternFilter.matches(binding.pattern()) && entryFilter.matches(binding.entry());
    }

    @Override
    public int hashCode() {
        return Objects.hash(patternFilter, entryFilter);
    }

}

