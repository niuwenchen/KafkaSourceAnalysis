package com.jackniu.kafka.common.network;

import com.jackniu.kafka.common.security.auth.SecurityProtocol;

import java.util.Locale;
import java.util.Objects;

public final class ListenerName {
    private static final String CONFIG_STATIC_PREFIX = "listener.name";

    /**
     * Create an instance with the security protocol name as the value.
     */
    public static ListenerName forSecurityProtocol(SecurityProtocol securityProtocol) {
        return new ListenerName(securityProtocol.name);
    }

    /**
     * Create an instance with the provided value converted to uppercase.
     */
    public static ListenerName normalised(String value) {
        return new ListenerName(value.toUpperCase(Locale.ROOT));
    }

    private final String value;

    public ListenerName(String value) {
        Objects.requireNonNull(value, "value should not be null");
        this.value = value;
    }

    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ListenerName))
            return false;
        ListenerName that = (ListenerName) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return "ListenerName(" + value + ")";
    }

    public String configPrefix() {
        return CONFIG_STATIC_PREFIX + "." + value.toLowerCase(Locale.ROOT) + ".";
    }

    public String saslMechanismConfigPrefix(String saslMechanism) {
        return configPrefix() + saslMechanismPrefix(saslMechanism);
    }

    public static String saslMechanismPrefix(String saslMechanism) {
        return saslMechanism.toLowerCase(Locale.ROOT) + ".";
    }
}
