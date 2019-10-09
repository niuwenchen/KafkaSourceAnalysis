package com.jackniu.kafka.common.security.auth;

import javax.security.sasl.SaslServer;
import java.net.InetAddress;

public class SaslAuthenticationContext implements AuthenticationContext {
    private final SaslServer server;
    private final SecurityProtocol securityProtocol;
    private final InetAddress clientAddress;
    private final String listenerName;

    public SaslAuthenticationContext(SaslServer server, SecurityProtocol securityProtocol, InetAddress clientAddress, String listenerName) {
        this.server = server;
        this.securityProtocol = securityProtocol;
        this.clientAddress = clientAddress;
        this.listenerName = listenerName;
    }

    public SaslServer server() {
        return server;
    }

    @Override
    public SecurityProtocol securityProtocol() {
        return securityProtocol;
    }

    @Override
    public InetAddress clientAddress() {
        return clientAddress;
    }

    @Override
    public String listenerName() {
        return listenerName;
    }
}

