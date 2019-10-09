package com.jackniu.kafka.common.security.auth;

import com.jackniu.kafka.common.Configurable;
import com.jackniu.kafka.common.KafkaException;
import com.jackniu.kafka.common.network.Authenticator;
import com.jackniu.kafka.common.network.TransportLayer;

import java.security.Principal;
import java.util.Map;

public interface PrincipalBuilder extends Configurable {

    /**
     * Configures this class with given key-value pairs.
     */
    void configure(Map<String, ?> configs);

    /**
     * Returns Principal.
     */
    Principal buildPrincipal(TransportLayer transportLayer, Authenticator authenticator) throws KafkaException;

    /**
     * Closes this instance.
     */
    void close() throws KafkaException;

}
