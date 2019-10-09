package com.jackniu.kafka.common.security.plain.internals;

import java.security.Provider;
import java.security.Security;

public class PlainSaslServerProvider extends Provider {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("deprecation")
    protected PlainSaslServerProvider() {
        super("Simple SASL/PLAIN Server Provider", 1.0, "Simple SASL/PLAIN Server Provider for Kafka");
        put("SaslServerFactory." + PlainSaslServer.PLAIN_MECHANISM, PlainSaslServer.PlainSaslServerFactory.class.getName());
    }

    public static void initialize() {
        Security.addProvider(new PlainSaslServerProvider());
    }
}

