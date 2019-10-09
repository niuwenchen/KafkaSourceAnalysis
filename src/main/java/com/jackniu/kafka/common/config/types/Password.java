package com.jackniu.kafka.common.config.types;

public class Password {
    public static final String HIDDEN = "[hidden]";

    private final String value;

    /**
     * Construct a new Password object
     * @param value The value of a password
     */
    public Password(String value) {
        this.value = value;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Password))
            return false;
        Password other = (Password) obj;
        return value.equals(other.value);
    }

    /**
     * Returns hidden password string
     *
     * @return hidden password string
     */
    @Override
    public String toString() {
        return HIDDEN;
    }

    /**
     * Returns real password string
     *
     * @return real password string
     */
    public String value() {
        return value;
    }

}
