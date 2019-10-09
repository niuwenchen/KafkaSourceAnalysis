package com.jackniu.kafka.common.security.auth;

import java.net.InetAddress;

public interface AuthenticationContext {
    /**
     * Underlying security protocol of the authentication session.
     */
    SecurityProtocol securityProtocol();

    /**
     * Address of the authenticated client
     */
    InetAddress clientAddress();

    /**
     * Name of the listener used for the connection
     */
    String listenerName();
}
