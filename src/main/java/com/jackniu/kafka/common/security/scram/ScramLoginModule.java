package com.jackniu.kafka.common.security.scram;

import com.jackniu.kafka.common.security.scram.internals.ScramSaslClientProvider;
import com.jackniu.kafka.common.security.scram.internals.ScramSaslServerProvider;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.spi.LoginModule;
import java.util.Collections;
import java.util.Map;

public class ScramLoginModule  implements LoginModule {

    private static final String USERNAME_CONFIG = "username";
    private static final String PASSWORD_CONFIG = "password";
    public static final String TOKEN_AUTH_CONFIG = "tokenauth";

    static {
        ScramSaslClientProvider.initialize();
        ScramSaslServerProvider.initialize();
    }

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        String username = (String) options.get(USERNAME_CONFIG);
        if (username != null)
            subject.getPublicCredentials().add(username);
        String password = (String) options.get(PASSWORD_CONFIG);
        if (password != null)
            subject.getPrivateCredentials().add(password);

        Boolean useTokenAuthentication = "true".equalsIgnoreCase((String) options.get(TOKEN_AUTH_CONFIG));
        if (useTokenAuthentication) {
            Map<String, String> scramExtensions = Collections.singletonMap(TOKEN_AUTH_CONFIG, "true");
            subject.getPublicCredentials().add(scramExtensions);
        }
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

