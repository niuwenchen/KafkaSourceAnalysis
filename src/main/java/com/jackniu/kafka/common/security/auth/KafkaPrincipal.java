package com.jackniu.kafka.common.security.auth;

import com.jackniu.kafka.common.utils.SecurityUtils;

import java.security.Principal;

import static java.util.Objects.requireNonNull;

public class KafkaPrincipal implements Principal {
    public static final String USER_TYPE = "User";
    public final static KafkaPrincipal ANONYMOUS = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "ANONYMOUS");

    private final String principalType;
    private final String name;
    private volatile boolean tokenAuthenticated;

    public KafkaPrincipal(String principalType, String name) {
        this.principalType = requireNonNull(principalType, "Principal type cannot be null");
        this.name = requireNonNull(name, "Principal name cannot be null");
    }

    /**
     * Parse a {@link KafkaPrincipal} instance from a string. This method cannot be used for {@link KafkaPrincipal}
     * extensions.
     *
     * @param str The input string formatted as "{principalType}:{principalName}"
     * @return The parsed {@link KafkaPrincipal} instance
     * @deprecated As of 1.0.0. This method will be removed in a future major release.
     */
    @Deprecated
    public static KafkaPrincipal fromString(String str) {
        return SecurityUtils.parseKafkaPrincipal(str);
    }

    @Override
    public String toString() {
        return principalType + ":" + name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (getClass() != o.getClass()) return false;

        KafkaPrincipal that = (KafkaPrincipal) o;
        return principalType.equals(that.principalType) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = principalType != null ? principalType.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getPrincipalType() {
        return principalType;
    }

    public void tokenAuthenticated(boolean tokenAuthenticated) {
        this.tokenAuthenticated = tokenAuthenticated;
    }

    public boolean tokenAuthenticated() {
        return tokenAuthenticated;
    }
}


