package com.jackniu.kafka.common.security.authenticator;

import com.jackniu.kafka.common.config.SaslConfigs;
import com.jackniu.kafka.common.security.AuthenticateCallbackHandler;
import com.jackniu.kafka.common.security.auth.SaslExtensions;
import com.jackniu.kafka.common.security.auth.SaslExtensionsCallback;
import com.jackniu.kafka.common.security.scram.ScramExtensionsCallback;
import com.jackniu.kafka.common.security.scram.internals.ScramMechanism;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import java.security.AccessController;
import java.util.List;
import java.util.Map;

public class SaslClientCallbackHandler implements AuthenticateCallbackHandler {

    private String mechanism;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        this.mechanism  = saslMechanism;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        Subject subject = Subject.getSubject(AccessController.getContext());
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                NameCallback nc = (NameCallback) callback;
                if (subject != null && !subject.getPublicCredentials(String.class).isEmpty()) {
                    nc.setName(subject.getPublicCredentials(String.class).iterator().next());
                } else
                    nc.setName(nc.getDefaultName());
            } else if (callback instanceof PasswordCallback) {
                if (subject != null && !subject.getPrivateCredentials(String.class).isEmpty()) {
                    char[] password = subject.getPrivateCredentials(String.class).iterator().next().toCharArray();
                    ((PasswordCallback) callback).setPassword(password);
                } else {
                    String errorMessage = "Could not login: the client is being asked for a password, but the Kafka" +
                            " client code does not currently support obtaining a password from the user.";
                    throw new UnsupportedCallbackException(callback, errorMessage);
                }
            } else if (callback instanceof RealmCallback) {
                RealmCallback rc = (RealmCallback) callback;
                rc.setText(rc.getDefaultText());
            } else if (callback instanceof AuthorizeCallback) {
                AuthorizeCallback ac = (AuthorizeCallback) callback;
                String authId = ac.getAuthenticationID();
                String authzId = ac.getAuthorizationID();
                ac.setAuthorized(authId.equals(authzId));
                if (ac.isAuthorized())
                    ac.setAuthorizedID(authzId);
            } else if (callback instanceof ScramExtensionsCallback) {
                if (ScramMechanism.isScram(mechanism) && subject != null && !subject.getPublicCredentials(Map.class).isEmpty()) {
                    @SuppressWarnings("unchecked")
                    Map<String, String> extensions = (Map<String, String>) subject.getPublicCredentials(Map.class).iterator().next();
                    ((ScramExtensionsCallback) callback).extensions(extensions);
                }
            } else if (callback instanceof SaslExtensionsCallback) {
                if (!SaslConfigs.GSSAPI_MECHANISM.equals(mechanism) &&
                        subject != null && !subject.getPublicCredentials(SaslExtensions.class).isEmpty()) {
                    SaslExtensions extensions = subject.getPublicCredentials(SaslExtensions.class).iterator().next();
                    ((SaslExtensionsCallback) callback).extensions(extensions);
                }
            }  else {
                throw new UnsupportedCallbackException(callback, "Unrecognized SASL ClientCallback");
            }
        }
    }

    @Override
    public void close() {
    }
}

