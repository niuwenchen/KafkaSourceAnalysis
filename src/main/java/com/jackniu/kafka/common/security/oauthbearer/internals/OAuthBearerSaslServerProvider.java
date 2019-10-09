package com.jackniu.kafka.common.security.oauthbearer.internals;

import com.jackniu.kafka.common.security.oauthbearer.OAuthBearerLoginModule;

import java.security.Provider;
import java.security.Security;

public class OAuthBearerSaslServerProvider extends Provider {
    private static final long serialVersionUID = 1L;

    protected OAuthBearerSaslServerProvider() {
        super("SASL/OAUTHBEARER Server Provider", 1.0, "SASL/OAUTHBEARER Server Provider for Kafka");
        put("SaslServerFactory." + OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                OAuthBearerSaslServer.OAuthBearerSaslServerFactory.class.getName());
    }

    public static void initialize() {
        Security.addProvider(new OAuthBearerSaslServerProvider());
    }
}
