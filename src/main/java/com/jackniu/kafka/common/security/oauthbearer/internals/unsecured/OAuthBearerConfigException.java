package com.jackniu.kafka.common.security.oauthbearer.internals.unsecured;

import com.jackniu.kafka.common.KafkaException;

public class OAuthBearerConfigException  extends KafkaException {
    private static final long serialVersionUID = -8056105648062343518L;

    public OAuthBearerConfigException(String s) {
        super(s);
    }

    public OAuthBearerConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
