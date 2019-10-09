package com.jackniu.kafka.common.security.scram.internals;

import com.jackniu.kafka.common.errors.IllegalSaslStateException;
import com.jackniu.kafka.common.security.scram.ScramExtensionsCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.*;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class ScramSaslClient implements SaslClient {

    private static final Logger log = LoggerFactory.getLogger(ScramSaslClient.class);

    enum State {
        SEND_CLIENT_FIRST_MESSAGE,
        RECEIVE_SERVER_FIRST_MESSAGE,
        RECEIVE_SERVER_FINAL_MESSAGE,
        COMPLETE,
        FAILED
    }

    private final ScramMechanism mechanism;
    private final CallbackHandler callbackHandler;
    private final ScramFormatter formatter;
    private String clientNonce;
    private State state;
    private byte[] saltedPassword;
    private ScramMessages.ClientFirstMessage clientFirstMessage;
    private ScramMessages.ServerFirstMessage serverFirstMessage;
    private ScramMessages.ClientFinalMessage clientFinalMessage;

    public ScramSaslClient(ScramMechanism mechanism, CallbackHandler cbh) throws NoSuchAlgorithmException {
        this.mechanism = mechanism;
        this.callbackHandler = cbh;
        this.formatter = new ScramFormatter(mechanism);
        setState(State.SEND_CLIENT_FIRST_MESSAGE);
    }

    @Override
    public String getMechanismName() {
        return mechanism.mechanismName();
    }

    @Override
    public boolean hasInitialResponse() {
        return true;
    }

    @Override
    public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
        try {
            switch (state) {
                case SEND_CLIENT_FIRST_MESSAGE:
                    if (challenge != null && challenge.length != 0)
                        throw new SaslException("Expected empty challenge");
                    clientNonce = formatter.secureRandomString();
                    NameCallback nameCallback = new NameCallback("Name:");
                    ScramExtensionsCallback extensionsCallback = new ScramExtensionsCallback();

                    try {
                        callbackHandler.handle(new Callback[]{nameCallback});
                        try {
                            callbackHandler.handle(new Callback[]{extensionsCallback});
                        } catch (UnsupportedCallbackException e) {
                            log.debug("Extensions callback is not supported by client callback handler {}, no extensions will be added",
                                    callbackHandler);
                        }
                    } catch (Throwable e) {
                        throw new SaslException("User name or extensions could not be obtained", e);
                    }

                    String username = nameCallback.getName();
                    String saslName = formatter.saslName(username);
                    Map<String, String> extensions = extensionsCallback.extensions();
                    this.clientFirstMessage = new ScramMessages.ClientFirstMessage(saslName, clientNonce, extensions);
                    setState(State.RECEIVE_SERVER_FIRST_MESSAGE);
                    return clientFirstMessage.toBytes();

                case RECEIVE_SERVER_FIRST_MESSAGE:
                    this.serverFirstMessage = new ScramMessages.ServerFirstMessage(challenge);
                    if (!serverFirstMessage.nonce().startsWith(clientNonce))
                        throw new SaslException("Invalid server nonce: does not start with client nonce");
                    if (serverFirstMessage.iterations() < mechanism.minIterations())
                        throw new SaslException("Requested iterations " + serverFirstMessage.iterations() +  " is less than the minimum " + mechanism.minIterations() + " for " + mechanism);
                    PasswordCallback passwordCallback = new PasswordCallback("Password:", false);
                    try {
                        callbackHandler.handle(new Callback[]{passwordCallback});
                    } catch (Throwable e) {
                        throw new SaslException("User name could not be obtained", e);
                    }
                    this.clientFinalMessage = handleServerFirstMessage(passwordCallback.getPassword());
                    setState(State.RECEIVE_SERVER_FINAL_MESSAGE);
                    return clientFinalMessage.toBytes();

                case RECEIVE_SERVER_FINAL_MESSAGE:
                    ScramMessages.ServerFinalMessage serverFinalMessage = new ScramMessages.ServerFinalMessage(challenge);
                    if (serverFinalMessage.error() != null)
                        throw new SaslException("Sasl authentication using " + mechanism + " failed with error: " + serverFinalMessage.error());
                    handleServerFinalMessage(serverFinalMessage.serverSignature());
                    setState(State.COMPLETE);
                    return null;

                default:
                    throw new IllegalSaslStateException("Unexpected challenge in Sasl client state " + state);
            }
        } catch (SaslException e) {
            setState(State.FAILED);
            throw e;
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
        log.debug("Setting SASL/{} client state to {}", mechanism, state);
        this.state = state;
    }

    private ScramMessages.ClientFinalMessage handleServerFirstMessage(char[] password) throws SaslException {
        try {
            byte[] passwordBytes = formatter.normalize(new String(password));
            this.saltedPassword = formatter.hi(passwordBytes, serverFirstMessage.salt(), serverFirstMessage.iterations());

            ScramMessages.ClientFinalMessage clientFinalMessage = new ScramMessages.ClientFinalMessage("n,,".getBytes(StandardCharsets.UTF_8), serverFirstMessage.nonce());
            byte[] clientProof = formatter.clientProof(saltedPassword, clientFirstMessage, serverFirstMessage, clientFinalMessage);
            clientFinalMessage.proof(clientProof);
            return clientFinalMessage;
        } catch (InvalidKeyException e) {
            throw new SaslException("Client final message could not be created", e);
        }
    }

    private void handleServerFinalMessage(byte[] signature) throws SaslException {
        try {
            byte[] serverKey = formatter.serverKey(saltedPassword);
            byte[] serverSignature = formatter.serverSignature(serverKey, clientFirstMessage, serverFirstMessage, clientFinalMessage);
            if (!Arrays.equals(signature, serverSignature))
                throw new SaslException("Invalid server signature in server final message");
        } catch (InvalidKeyException e) {
            throw new SaslException("Sasl server signature verification failed", e);
        }
    }

    public static class ScramSaslClientFactory implements SaslClientFactory {

        @Override
        public SaslClient createSaslClient(String[] mechanisms,
                                           String authorizationId,
                                           String protocol,
                                           String serverName,
                                           Map<String, ?> props,
                                           CallbackHandler cbh) throws SaslException {

            ScramMechanism mechanism = null;
            for (String mech : mechanisms) {
                mechanism = ScramMechanism.forMechanismName(mech);
                if (mechanism != null)
                    break;
            }
            if (mechanism == null)
                throw new SaslException(String.format("Requested mechanisms '%s' not supported. Supported mechanisms are '%s'.",
                        Arrays.asList(mechanisms), ScramMechanism.mechanismNames()));

            try {
                return new ScramSaslClient(mechanism, cbh);
            } catch (NoSuchAlgorithmException e) {
                throw new SaslException("Hash algorithm not supported for mechanism " + mechanism, e);
            }
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            Collection<String> mechanisms = ScramMechanism.mechanismNames();
            return mechanisms.toArray(new String[mechanisms.size()]);
        }
    }
}

