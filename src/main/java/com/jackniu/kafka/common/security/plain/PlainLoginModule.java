package com.jackniu.kafka.common.security.plain;

import com.jackniu.kafka.common.security.plain.internals.PlainSaslServerProvider;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.spi.LoginModule;
import java.util.Map;

public class PlainLoginModule implements LoginModule {

    private static final String USERNAME_CONFIG = "username";
    private static final String PASSWORD_CONFIG = "password";

    static {
        PlainSaslServerProvider.initialize();
    }

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        String username = (String) options.get(USERNAME_CONFIG);
        if (username != null)
            subject.getPublicCredentials().add(username);
        String password = (String) options.get(PASSWORD_CONFIG);
        if (password != null)
            subject.getPrivateCredentials().add(password);
    }

    @Override
    public boolean login() {
        return true;
    }

    @Override
    public boolean logout() {
        return true;
    }

    @Override
    public boolean commit() {
        return true;
    }

    @Override
    public boolean abort() {
        return false;
    }
}

