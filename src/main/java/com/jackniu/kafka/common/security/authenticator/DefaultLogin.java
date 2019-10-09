package com.jackniu.kafka.common.security.authenticator;

public class DefaultLogin  extends AbstractLogin {

    @Override
    public String serviceName() {
        return "kafka";
    }

    @Override
    public void close() {
    }
}


