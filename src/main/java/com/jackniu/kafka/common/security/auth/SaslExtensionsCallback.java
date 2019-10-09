package com.jackniu.kafka.common.security.auth;

import javax.security.auth.callback.Callback;
import java.util.Objects;

public class SaslExtensionsCallback implements Callback {
    private SaslExtensions extensions = SaslExtensions.NO_SASL_EXTENSIONS;

    /**
     * Returns always non-null {@link SaslExtensions} consisting of the extension
     * names and values that are sent by the client to the server in the initial
     * client SASL authentication message. The default value is
     * {@link SaslExtensions#NO_SASL_EXTENSIONS} so that if this callback is
     * unhandled the client will see a non-null value.
     */
    public SaslExtensions extensions() {
        return extensions;
    }

    /**
     * Sets the SASL extensions on this callback.
     *
     * @param extensions
     *            the mandatory extensions to set
     */
    public void extensions(SaslExtensions extensions) {
        this.extensions = Objects.requireNonNull(extensions, "extensions must not be null");
    }
}

