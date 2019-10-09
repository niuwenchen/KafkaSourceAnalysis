package com.jackniu.kafka.common.security.scram.internals;

import java.security.Provider;
import java.security.Security;

public class ScramSaslServerProvider extends Provider {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("deprecation")
    protected ScramSaslServerProvider() {
        super("SASL/SCRAM Server Provider", 1.0, "SASL/SCRAM Server Provider for Kafka");
        for (ScramMechanism mechanism : ScramMechanism.values())
            put("SaslServerFactory." + mechanism.mechanismName(), ScramSaslServer.ScramSaslServerFactory.class.getName());
    }

    public static void initialize() {
        Security.addProvider(new ScramSaslServerProvider());
    }
}

