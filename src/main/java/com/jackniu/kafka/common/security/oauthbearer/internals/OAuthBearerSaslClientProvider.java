package com.jackniu.kafka.common.security.oauthbearer.internals;

import com.jackniu.kafka.common.security.oauthbearer.OAuthBearerLoginModule;

import java.security.Provider;
import java.security.Security;

public class OAuthBearerSaslClientProvider extends Provider {
    private static final long serialVersionUID = 1L;

    protected OAuthBearerSaslClientProvider() {
        super("SASL/OAUTHBEARER Client Provider", 1.0, "SASL/OAUTHBEARER Client Provider for Kafka");
        put("SaslClientFactory." + OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                OAuthBearerSaslClient.OAuthBearerSaslClientFactory.class.getName());
    }

    public static void initialize() {
        Security.addProvider(new OAuthBearerSaslClientProvider());
    }
}
