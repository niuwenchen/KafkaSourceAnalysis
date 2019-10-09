package com.jackniu.kafka.common.security.scram.internals;

import com.jackniu.kafka.common.security.AuthenticateCallbackHandler;
import com.jackniu.kafka.common.security.authenticator.CredentialCache;
import com.jackniu.kafka.common.security.scram.ScramCredential;
import com.jackniu.kafka.common.security.scram.ScramCredentialCallback;
import com.jackniu.kafka.common.security.token.delegation.TokenInformation;
import com.jackniu.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import com.jackniu.kafka.common.security.token.delegation.internals.DelegationTokenCredentialCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.util.List;
import java.util.Map;

public class ScramServerCallbackHandler implements AuthenticateCallbackHandler {

    private final CredentialCache.Cache<ScramCredential> credentialCache;
    private final DelegationTokenCache tokenCache;
    private String saslMechanism;

    public ScramServerCallbackHandler(CredentialCache.Cache<ScramCredential> credentialCache,
                                      DelegationTokenCache tokenCache) {
        this.credentialCache = credentialCache;
        this.tokenCache = tokenCache;
    }

    @Override
    public void configure(Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        this.saslMechanism = mechanism;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        String username = null;
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback)
                username = ((NameCallback) callback).getDefaultName();
            else if (callback instanceof DelegationTokenCredentialCallback) {
                DelegationTokenCredentialCallback tokenCallback = (DelegationTokenCredentialCallback) callback;
                tokenCallback.scramCredential(tokenCache.credential(saslMechanism, username));
                tokenCallback.tokenOwner(tokenCache.owner(username));
                TokenInformation tokenInfo = tokenCache.token(username);
                if (tokenInfo != null)
                    tokenCallback.tokenExpiryTimestamp(tokenInfo.expiryTimestamp());
            } else if (callback instanceof ScramCredentialCallback) {
                ScramCredentialCallback sc = (ScramCredentialCallback) callback;
                sc.scramCredential(credentialCache.get(username));
            } else
                throw new UnsupportedCallbackException(callback);
        }
    }

    @Override
    public void close() {
    }
}
