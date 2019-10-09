package com.jackniu.kafka.clients;

public enum ConnectionState  {
    DISCONNECTED, CONNECTING, CHECKING_API_VERSIONS, READY, AUTHENTICATION_FAILED;

    public boolean isDisconnected() {
        return this == AUTHENTICATION_FAILED || this == DISCONNECTED;
    }

    public boolean isConnected() {
        return this == CHECKING_API_VERSIONS || this == READY;
    }

}
