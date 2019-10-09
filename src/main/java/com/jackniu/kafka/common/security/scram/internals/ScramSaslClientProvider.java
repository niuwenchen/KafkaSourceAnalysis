package com.jackniu.kafka.common.security.scram.internals;


import java.security.Provider;
import java.security.Security;

public class ScramSaslClientProvider extends Provider {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("deprecation")
    protected ScramSaslClientProvider() {
        super("SASL/SCRAM Client Provider", 1.0, "SASL/SCRAM Client Provider for Kafka");
        for (ScramMechanism mechanism : ScramMechanism.values())
            put("SaslClientFactory." + mechanism.mechanismName(), ScramSaslClient.ScramSaslClientFactory.class.getName());
    }

    public static void initialize() {
        Security.addProvider(new ScramSaslClientProvider());
    }
}

