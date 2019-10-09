package com.jackniu.kafka.common.security.auth;

import javax.net.ssl.SSLSession;
import java.net.InetAddress;

public class SslAuthenticationContext  implements AuthenticationContext {
    private final SSLSession session;
    private final InetAddress clientAddress;
    private final String listenerName;

    public SslAuthenticationContext(SSLSession session, InetAddress clientAddress, String listenerName) {
        this.session = session;
        this.clientAddress = clientAddress;
        this.listenerName = listenerName;
    }

    public SSLSession session() {
        return session;
    }

    @Override
    public SecurityProtocol securityProtocol() {
        return SecurityProtocol.SSL;
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
