package com.jackniu.kafka.common.security.auth;

import com.jackniu.kafka.common.KafkaException;
import com.jackniu.kafka.common.network.Authenticator;
import com.jackniu.kafka.common.network.TransportLayer;

import java.security.Principal;
import java.util.Map;

public class DefaultPrincipalBuilder implements PrincipalBuilder {

    public void configure(Map<String, ?> configs) {}

    public Principal buildPrincipal(TransportLayer transportLayer, Authenticator authenticator) throws KafkaException {
        try {
            return transportLayer.peerPrincipal();
        } catch (Exception e) {
            throw new KafkaException("Failed to build principal due to: ", e);
        }
    }

    public void close() throws KafkaException {}

}
