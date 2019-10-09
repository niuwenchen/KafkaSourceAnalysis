package com.jackniu.kafka.common.security.plain.internals;

import com.jackniu.kafka.common.KafkaException;
import com.jackniu.kafka.common.security.AuthenticateCallbackHandler;
import com.jackniu.kafka.common.security.JaasContext;
import com.jackniu.kafka.common.security.plain.PlainAuthenticateCallback;
import com.jackniu.kafka.common.security.plain.PlainLoginModule;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PlainServerCallbackHandler implements AuthenticateCallbackHandler {

    private static final String JAAS_USER_PREFIX = "user_";
    private List<AppConfigurationEntry> jaasConfigEntries;

    @Override
    public void configure(Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        this.jaasConfigEntries = jaasConfigEntries;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        String username = null;
        for (Callback callback: callbacks) {
            if (callback instanceof NameCallback)
                username = ((NameCallback) callback).getDefaultName();
            else if (callback instanceof PlainAuthenticateCallback) {
                PlainAuthenticateCallback plainCallback = (PlainAuthenticateCallback) callback;
                boolean authenticated = authenticate(username, plainCallback.password());
                plainCallback.authenticated(authenticated);
            } else
                throw new UnsupportedCallbackException(callback);
        }
    }

    protected boolean authenticate(String username, char[] password) throws IOException {
        if (username == null)
            return false;
        else {
            String expectedPassword = JaasContext.configEntryOption(jaasConfigEntries,
                    JAAS_USER_PREFIX + username,
                    PlainLoginModule.class.getName());
            return expectedPassword != null && Arrays.equals(password, expectedPassword.toCharArray());
        }
    }

    @Override
    public void close() throws KafkaException {
    }

}

