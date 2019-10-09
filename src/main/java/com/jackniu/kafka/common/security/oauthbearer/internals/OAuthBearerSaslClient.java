package com.jackniu.kafka.common.security.oauthbearer.internals;

import com.jackniu.kafka.common.errors.IllegalSaslStateException;
import com.jackniu.kafka.common.security.AuthenticateCallbackHandler;
import com.jackniu.kafka.common.security.auth.SaslExtensions;
import com.jackniu.kafka.common.security.auth.SaslExtensionsCallback;
import com.jackniu.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import com.jackniu.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class OAuthBearerSaslClient implements SaslClient {
    static final byte BYTE_CONTROL_A = (byte) 0x01;
    private static final Logger log = LoggerFactory.getLogger(OAuthBearerSaslClient.class);
    private final CallbackHandler callbackHandler;

    enum State {
        SEND_CLIENT_FIRST_MESSAGE, RECEIVE_SERVER_FIRST_MESSAGE, RECEIVE_SERVER_MESSAGE_AFTER_FAILURE, COMPLETE, FAILED
    }

    private State state;

    public OAuthBearerSaslClient(AuthenticateCallbackHandler callbackHandler) {
        this.callbackHandler = Objects.requireNonNull(callbackHandler);
        setState(State.SEND_CLIENT_FIRST_MESSAGE);
    }

    public CallbackHandler callbackHandler() {
        return callbackHandler;
    }

    @Override
    public String getMechanismName() {
        return OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
    }

    @Override
    public boolean hasInitialResponse() {
        return true;
    }

    @Override
    public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
        try {
            OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
            switch (state) {
                case SEND_CLIENT_FIRST_MESSAGE:
                    if (challenge != null && challenge.length != 0)
                        throw new SaslException("Expected empty challenge");
                    callbackHandler().handle(new Callback[] {callback});
                    SaslExtensions extensions = retrieveCustomExtensions();

                    setState(State.RECEIVE_SERVER_FIRST_MESSAGE);

                    return new OAuthBearerClientInitialResponse(callback.token().value(), extensions).toBytes();
                case RECEIVE_SERVER_FIRST_MESSAGE:
                    if (challenge != null && challenge.length != 0) {
                        String jsonErrorResponse = new String(challenge, StandardCharsets.UTF_8);
                        if (log.isDebugEnabled())
                            log.debug("Sending %%x01 response to server after receiving an error: {}",
                                    jsonErrorResponse);
                        setState(State.RECEIVE_SERVER_MESSAGE_AFTER_FAILURE);
                        return new byte[] {BYTE_CONTROL_A};
                    }
                    callbackHandler().handle(new Callback[] {callback});
                    if (log.isDebugEnabled())
                        log.debug("Successfully authenticated as {}", callback.token().principalName());
                    setState(State.COMPLETE);
                    return null;
                default:
                    throw new IllegalSaslStateException("Unexpected challenge in Sasl client state " + state);
            }
        } catch (SaslException e) {
            setState(State.FAILED);
            throw e;
        } catch (IOException | UnsupportedCallbackException e) {
            setState(State.FAILED);
            throw new SaslException(e.getMessage(), e);
        }
    }

    @Override
    public boolean isComplete() {
        return state == State.COMPLETE;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) {
        if (!isComplete())
            throw new IllegalStateException("Authentication exchange has not completed");
        return Arrays.copyOfRange(incoming, offset, offset + len);
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) {
        if (!isComplete())
            throw new IllegalStateException("Authentication exchange has not completed");
        return Arrays.copyOfRange(outgoing, offset, offset + len);
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        if (!isComplete())
            throw new IllegalStateException("Authentication exchange has not completed");
        return null;
    }

    @Override
    public void dispose() {
    }

    private void setState(State state) {
        log.debug("Setting SASL/{} client state to {}", OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, state);
        this.state = state;
    }

    private SaslExtensions retrieveCustomExtensions() throws SaslException {
        SaslExtensionsCallback extensionsCallback = new SaslExtensionsCallback();
        try {
            callbackHandler().handle(new Callback[] {extensionsCallback});
        } catch (UnsupportedCallbackException e) {
            log.debug("Extensions callback is not supported by client callback handler {}, no extensions will be added",
                    callbackHandler());
        } catch (Exception e) {
            throw new SaslException("SASL extensions could not be obtained", e);
        }

        return extensionsCallback.extensions();
    }

    public static class OAuthBearerSaslClientFactory implements SaslClientFactory {
        @Override
        public SaslClient createSaslClient(String[] mechanisms, String authorizationId, String protocol,
                                           String serverName, Map<String, ?> props, CallbackHandler callbackHandler) {
            String[] mechanismNamesCompatibleWithPolicy = getMechanismNames(props);
            for (String mechanism : mechanisms) {
                for (int i = 0; i < mechanismNamesCompatibleWithPolicy.length; i++) {
                    if (mechanismNamesCompatibleWithPolicy[i].equals(mechanism)) {
                        if (!(Objects.requireNonNull(callbackHandler) instanceof AuthenticateCallbackHandler))
                            throw new IllegalArgumentException(String.format(
                                    "Callback handler must be castable to %s: %s",
                                    AuthenticateCallbackHandler.class.getName(), callbackHandler.getClass().getName()));
                        return new OAuthBearerSaslClient((AuthenticateCallbackHandler) callbackHandler);
                    }
                }
            }
            return null;
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            return OAuthBearerSaslServer.mechanismNamesCompatibleWithPolicy(props);
        }
    }
}

