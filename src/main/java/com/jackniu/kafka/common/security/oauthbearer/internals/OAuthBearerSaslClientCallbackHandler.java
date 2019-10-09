package com.jackniu.kafka.common.security.oauthbearer.internals;

import com.jackniu.kafka.common.security.AuthenticateCallbackHandler;
import com.jackniu.kafka.common.security.auth.SaslExtensions;
import com.jackniu.kafka.common.security.auth.SaslExtensionsCallback;
import com.jackniu.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import com.jackniu.kafka.common.security.oauthbearer.OAuthBearerToken;
import com.jackniu.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.security.AccessController;
import java.util.*;

public class OAuthBearerSaslClientCallbackHandler implements AuthenticateCallbackHandler {
    private static final Logger log = LoggerFactory.getLogger(OAuthBearerSaslClientCallbackHandler.class);
    private boolean configured = false;

    /**
     * Return true if this instance has been configured, otherwise false
     *
     * @return true if this instance has been configured, otherwise false
     */
    public boolean configured() {
        return configured;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism))
            throw new IllegalArgumentException(String.format("Unexpected SASL mechanism: %s", saslMechanism));
        configured = true;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        if (!configured())
            throw new IllegalStateException("Callback handler not configured");
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback)
                handleCallback((OAuthBearerTokenCallback) callback);
            else if (callback instanceof SaslExtensionsCallback)
                handleCallback((SaslExtensionsCallback) callback, Subject.getSubject(AccessController.getContext()));
            else
                throw new UnsupportedCallbackException(callback);
        }
    }

    @Override
    public void close() {
        // empty
    }

    private void handleCallback(OAuthBearerTokenCallback callback) throws IOException {
        if (callback.token() != null)
            throw new IllegalArgumentException("Callback had a token already");
        Subject subject = Subject.getSubject(AccessController.getContext());
        Set<OAuthBearerToken> privateCredentials = subject != null
                ? subject.getPrivateCredentials(OAuthBearerToken.class)
                : Collections.emptySet();
        if (privateCredentials.size() == 0)
            throw new IOException("No OAuth Bearer tokens in Subject's private credentials");
        if (privateCredentials.size() == 1)
            callback.token(privateCredentials.iterator().next());
        else {
            /*
             * There a very small window of time upon token refresh (on the order of milliseconds)
             * where both an old and a new token appear on the Subject's private credentials.
             * Rather than implement a lock to eliminate this window, we will deal with it by
             * checking for the existence of multiple tokens and choosing the one that has the
             * longest lifetime.  It is also possible that a bug could cause multiple tokens to
             * exist (e.g. KAFKA-7902), so dealing with the unlikely possibility that occurs
             * during normal operation also allows us to deal more robustly with potential bugs.
             */
            SortedSet<OAuthBearerToken> sortedByLifetime =
                    new TreeSet<>(
                            new Comparator<OAuthBearerToken>() {
                                @Override
                                public int compare(OAuthBearerToken o1, OAuthBearerToken o2) {
                                    return Long.compare(o1.lifetimeMs(), o2.lifetimeMs());
                                }
                            });
            sortedByLifetime.addAll(privateCredentials);
            log.warn("Found {} OAuth Bearer tokens in Subject's private credentials; the oldest expires at {}, will use the newest, which expires at {}",
                    sortedByLifetime.size(),
                    new Date(sortedByLifetime.first().lifetimeMs()),
                    new Date(sortedByLifetime.last().lifetimeMs()));
            callback.token(sortedByLifetime.last());
        }
    }

    /**
     * Attaches the first {@link SaslExtensions} found in the public credentials of the Subject
     */
    private static void handleCallback(SaslExtensionsCallback extensionsCallback, Subject subject) {
        if (subject != null && !subject.getPublicCredentials(SaslExtensions.class).isEmpty()) {
            SaslExtensions extensions = subject.getPublicCredentials(SaslExtensions.class).iterator().next();
            extensionsCallback.extensions(extensions);
        }
    }
}

