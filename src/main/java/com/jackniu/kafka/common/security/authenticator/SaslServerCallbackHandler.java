package com.jackniu.kafka.common.security.authenticator;

import com.jackniu.kafka.common.config.SaslConfigs;
import com.jackniu.kafka.common.security.AuthenticateCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import java.util.List;
import java.util.Map;

public class SaslServerCallbackHandler implements AuthenticateCallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SaslServerCallbackHandler.class);

    private String mechanism;

    @Override
    public void configure(Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        this.mechanism = mechanism;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof RealmCallback)
                handleRealmCallback((RealmCallback) callback);
            else if (callback instanceof AuthorizeCallback && mechanism.equals(SaslConfigs.GSSAPI_MECHANISM))
                handleAuthorizeCallback((AuthorizeCallback) callback);
            else
                throw new UnsupportedCallbackException(callback);
        }
    }

    private void handleRealmCallback(RealmCallback rc) {
        LOG.trace("Client supplied realm: {} ", rc.getDefaultText());
        rc.setText(rc.getDefaultText());
    }

    private void handleAuthorizeCallback(AuthorizeCallback ac) {
        String authenticationID = ac.getAuthenticationID();
        String authorizationID = ac.getAuthorizationID();
        LOG.info("Successfully authenticated client: authenticationID={}; authorizationID={}.",
                authenticationID, authorizationID);
        ac.setAuthorized(true);
        ac.setAuthorizedID(authenticationID);
    }

    @Override
    public void close() {
    }
}

