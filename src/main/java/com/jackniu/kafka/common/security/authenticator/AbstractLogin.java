package com.jackniu.kafka.common.security.authenticator;

import com.jackniu.kafka.common.security.AuthenticateCallbackHandler;
import com.jackniu.kafka.common.security.auth.Login;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.RealmCallback;
import java.util.List;
import java.util.Map;

public abstract class AbstractLogin implements Login {
    private static final Logger log = LoggerFactory.getLogger(AbstractLogin.class);

    private String contextName;
    private Configuration configuration;
    private LoginContext loginContext;
    private AuthenticateCallbackHandler loginCallbackHandler;

    @Override
    public void configure(Map<String, ?> configs, String contextName, Configuration configuration,
                          AuthenticateCallbackHandler loginCallbackHandler) {
        this.contextName = contextName;
        this.configuration = configuration;
        this.loginCallbackHandler = loginCallbackHandler;
    }

    @Override
    public LoginContext login() throws LoginException {
        loginContext = new LoginContext(contextName, null, loginCallbackHandler, configuration);
        loginContext.login();
        log.info("Successfully logged in.");
        return loginContext;
    }

    @Override
    public Subject subject() {
        return loginContext.getSubject();
    }

    protected String contextName() {
        return contextName;
    }

    protected Configuration configuration() {
        return configuration;
    }

    /**
     * Callback handler for creating login context. Login callback handlers
     * should support the callbacks required for the login modules used by
     * the KafkaServer and KafkaClient contexts. Kafka does not support
     * callback handlers which require additional user input.
     *
     */
    public static class DefaultLoginCallbackHandler implements AuthenticateCallbackHandler {

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nc = (NameCallback) callback;
                    nc.setName(nc.getDefaultName());
                } else if (callback instanceof PasswordCallback) {
                    String errorMessage = "Could not login: the client is being asked for a password, but the Kafka" +
                            " client code does not currently support obtaining a password from the user.";
                    throw new UnsupportedCallbackException(callback, errorMessage);
                } else if (callback instanceof RealmCallback) {
                    RealmCallback rc = (RealmCallback) callback;
                    rc.setText(rc.getDefaultText());
                } else {
                    throw new UnsupportedCallbackException(callback, "Unrecognized SASL Login callback");
                }
            }
        }

        @Override
        public void close() {
        }
    }
}

