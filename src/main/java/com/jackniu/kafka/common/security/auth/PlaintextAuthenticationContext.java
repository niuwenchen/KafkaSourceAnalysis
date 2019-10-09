package com.jackniu.kafka.common.security.auth;

import java.net.InetAddress;

public class PlaintextAuthenticationContext implements AuthenticationContext {
    private final InetAddress clientAddress;
    private final String listenerName;

    public PlaintextAuthenticationContext(InetAddress clientAddress, String listenerName) {
        this.clientAddress = clientAddress;
        this.listenerName = listenerName;
    }

    @Override
    public SecurityProtocol securityProtocol() {
        return SecurityProtocol.PLAINTEXT;
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

