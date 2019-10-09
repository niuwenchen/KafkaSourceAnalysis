package com.jackniu.kafka.common.network;

import com.jackniu.kafka.common.KafkaException;
import com.jackniu.kafka.common.config.SaslConfigs;
import com.jackniu.kafka.common.config.SslConfigs;
import com.jackniu.kafka.common.config.internal.BrokerSecurityConfigs;
import com.jackniu.kafka.common.memory.MemoryPool;
import com.jackniu.kafka.common.security.AuthenticateCallbackHandler;
import com.jackniu.kafka.common.security.JaasContext;
import com.jackniu.kafka.common.security.auth.Login;
import com.jackniu.kafka.common.security.auth.SecurityProtocol;
import com.jackniu.kafka.common.security.authenticator.*;
import com.jackniu.kafka.common.security.kerberos.KerberosClientCallbackHandler;
import com.jackniu.kafka.common.security.kerberos.KerberosLogin;
import com.jackniu.kafka.common.security.kerberos.KerberosShortNamer;
import com.jackniu.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import com.jackniu.kafka.common.security.oauthbearer.internals.OAuthBearerRefreshingLogin;
import com.jackniu.kafka.common.security.oauthbearer.internals.OAuthBearerSaslClientCallbackHandler;
import com.jackniu.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredValidatorCallbackHandler;
import com.jackniu.kafka.common.security.plain.internals.PlainSaslServer;
import com.jackniu.kafka.common.security.plain.internals.PlainServerCallbackHandler;
import com.jackniu.kafka.common.security.scram.ScramCredential;
import com.jackniu.kafka.common.security.scram.internals.ScramMechanism;
import com.jackniu.kafka.common.security.scram.internals.ScramServerCallbackHandler;
import com.jackniu.kafka.common.security.ssl.SslFactory;
import com.jackniu.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import com.jackniu.kafka.common.utils.Java;
import com.jackniu.kafka.common.utils.Time;
import com.jackniu.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.function.Supplier;

public class SaslChannelBuilder  implements ChannelBuilder, ListenerReconfigurable {
    private static final Logger log = LoggerFactory.getLogger(SaslChannelBuilder.class);

    private final SecurityProtocol securityProtocol;
    private final ListenerName listenerName;
    private final boolean isInterBrokerListener;
    private final String clientSaslMechanism;
    private final Mode mode;
    private final Map<String, JaasContext> jaasContexts;
    private final boolean handshakeRequestEnable;
    private final CredentialCache credentialCache;
    private final DelegationTokenCache tokenCache;
    private final Map<String, LoginManager> loginManagers;
    private final Map<String, Subject> subjects;

    private SslFactory sslFactory;
    private Map<String, ?> configs;
    private KerberosShortNamer kerberosShortNamer;
    private Map<String, AuthenticateCallbackHandler> saslCallbackHandlers;
    private Map<String, Long> connectionsMaxReauthMsByMechanism;
    private final Time time;

    public SaslChannelBuilder(Mode mode,
                              Map<String, JaasContext> jaasContexts,
                              SecurityProtocol securityProtocol,
                              ListenerName listenerName,
                              boolean isInterBrokerListener,
                              String clientSaslMechanism,
                              boolean handshakeRequestEnable,
                              CredentialCache credentialCache,
                              DelegationTokenCache tokenCache,
                              Time time) {
        this.mode = mode;
        this.jaasContexts = jaasContexts;
        this.loginManagers = new HashMap<>(jaasContexts.size());
        this.subjects = new HashMap<>(jaasContexts.size());
        this.securityProtocol = securityProtocol;
        this.listenerName = listenerName;
        this.isInterBrokerListener = isInterBrokerListener;
        this.handshakeRequestEnable = handshakeRequestEnable;
        this.clientSaslMechanism = clientSaslMechanism;
        this.credentialCache = credentialCache;
        this.tokenCache = tokenCache;
        this.saslCallbackHandlers = new HashMap<>();
        this.connectionsMaxReauthMsByMechanism = new HashMap<>();
        this.time = time;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) throws KafkaException {
        try {
            this.configs = configs;
            if (mode == Mode.SERVER) {
                createServerCallbackHandlers(configs);
                createConnectionsMaxReauthMsMap(configs);
            } else
                createClientCallbackHandler(configs);
            for (Map.Entry<String, AuthenticateCallbackHandler> entry : saslCallbackHandlers.entrySet()) {
                String mechanism = entry.getKey();
                entry.getValue().configure(configs, mechanism, jaasContexts.get(mechanism).configurationEntries());
            }

            Class<? extends Login> defaultLoginClass = defaultLoginClass(configs);
            if (jaasContexts.containsKey(SaslConfigs.GSSAPI_MECHANISM)) {
                String defaultRealm;
                try {
                    defaultRealm = defaultKerberosRealm();
                } catch (Exception ke) {
                    defaultRealm = "";
                }
                List<String> principalToLocalRules = (List<String>) configs.get(BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG);
                if (principalToLocalRules != null)
                    kerberosShortNamer = KerberosShortNamer.fromUnparsedRules(defaultRealm, principalToLocalRules);
            }
            for (Map.Entry<String, JaasContext> entry : jaasContexts.entrySet()) {
                String mechanism = entry.getKey();
                // With static JAAS configuration, use KerberosLogin if Kerberos is enabled. With dynamic JAAS configuration,
                // use KerberosLogin only for the LoginContext corresponding to GSSAPI
                LoginManager loginManager = LoginManager.acquireLoginManager(entry.getValue(), mechanism, defaultLoginClass, configs);
                loginManagers.put(mechanism, loginManager);
                subjects.put(mechanism, loginManager.subject());
            }
            if (this.securityProtocol == SecurityProtocol.SASL_SSL) {
                // Disable SSL client authentication as we are using SASL authentication
                this.sslFactory = new SslFactory(mode, "none", isInterBrokerListener);
                this.sslFactory.configure(configs);
            }
        } catch (Throwable e) {
            close();
            throw new KafkaException(e);
        }
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return securityProtocol == SecurityProtocol.SASL_SSL ? SslConfigs.RECONFIGURABLE_CONFIGS : Collections.emptySet();
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) {
        if (this.securityProtocol == SecurityProtocol.SASL_SSL)
            sslFactory.validateReconfiguration(configs);
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        if (this.securityProtocol == SecurityProtocol.SASL_SSL)
            sslFactory.reconfigure(configs);
    }

    @Override
    public ListenerName listenerName() {
        return listenerName;
    }

    @Override
    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize, MemoryPool memoryPool) throws KafkaException {
        try {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            Socket socket = socketChannel.socket();
            TransportLayer transportLayer = buildTransportLayer(id, key, socketChannel);
            Supplier<Authenticator> authenticatorCreator;
            if (mode == Mode.SERVER) {
                authenticatorCreator = () -> buildServerAuthenticator(configs,
                        Collections.unmodifiableMap(saslCallbackHandlers),
                        id,
                        transportLayer,
                        Collections.unmodifiableMap(subjects),
                        Collections.unmodifiableMap(connectionsMaxReauthMsByMechanism));
            } else {
                LoginManager loginManager = loginManagers.get(clientSaslMechanism);
                authenticatorCreator = () -> buildClientAuthenticator(configs,
                        saslCallbackHandlers.get(clientSaslMechanism),
                        id,
                        socket.getInetAddress().getHostName(),
                        loginManager.serviceName(),
                        transportLayer,
                        subjects.get(clientSaslMechanism));
            }
            return new KafkaChannel(id, transportLayer, authenticatorCreator, maxReceiveSize, memoryPool != null ? memoryPool : MemoryPool.NONE);
        } catch (Exception e) {
            log.info("Failed to create channel due to ", e);
            throw new KafkaException(e);
        }
    }

    @Override
    public void close()  {
        for (LoginManager loginManager : loginManagers.values())
            loginManager.release();
        loginManagers.clear();
        for (AuthenticateCallbackHandler handler : saslCallbackHandlers.values())
            handler.close();
    }

    // Visible to override for testing
    protected TransportLayer buildTransportLayer(String id, SelectionKey key, SocketChannel socketChannel) throws IOException {
        if (this.securityProtocol == SecurityProtocol.SASL_SSL) {
            return SslTransportLayer.create(id, key,
                    sslFactory.createSslEngine(socketChannel.socket().getInetAddress().getHostName(), socketChannel.socket().getPort()));
        } else {
            return new PlaintextTransportLayer(key);
        }
    }

    // Visible to override for testing
    protected SaslServerAuthenticator buildServerAuthenticator(Map<String, ?> configs,
                                                               Map<String, AuthenticateCallbackHandler> callbackHandlers,
                                                               String id,
                                                               TransportLayer transportLayer,
                                                               Map<String, Subject> subjects,
                                                               Map<String, Long> connectionsMaxReauthMsByMechanism) {
        return new SaslServerAuthenticator(configs, callbackHandlers, id, subjects,
                kerberosShortNamer, listenerName, securityProtocol, transportLayer, connectionsMaxReauthMsByMechanism, time);
    }

    // Visible to override for testing
    protected SaslClientAuthenticator buildClientAuthenticator(Map<String, ?> configs,
                                                               AuthenticateCallbackHandler callbackHandler,
                                                               String id,
                                                               String serverHost,
                                                               String servicePrincipal,
                                                               TransportLayer transportLayer, Subject subject) {
        return new SaslClientAuthenticator(configs, callbackHandler, id, subject, servicePrincipal,
                serverHost, clientSaslMechanism, handshakeRequestEnable, transportLayer, time);
    }

    // Package private for testing
    Map<String, LoginManager> loginManagers() {
        return loginManagers;
    }

    private static String defaultKerberosRealm() throws ClassNotFoundException, NoSuchMethodException,
            IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        //TODO Find a way to avoid using these proprietary classes as access to Java 9 will block access by default
        //due to the Jigsaw module system

        Object kerbConf;
        Class<?> classRef;
        Method getInstanceMethod;
        Method getDefaultRealmMethod;
        if (Java.isIbmJdk()) {
            classRef = Class.forName("com.ibm.security.krb5.internal.Config");
        } else {
            classRef = Class.forName("sun.security.krb5.Config");
        }
        getInstanceMethod = classRef.getMethod("getInstance", new Class[0]);
        kerbConf = getInstanceMethod.invoke(classRef, new Object[0]);
        getDefaultRealmMethod = classRef.getDeclaredMethod("getDefaultRealm", new Class[0]);
        return (String) getDefaultRealmMethod.invoke(kerbConf, new Object[0]);
    }

    private void createClientCallbackHandler(Map<String, ?> configs) {
        @SuppressWarnings("unchecked")
        Class<? extends AuthenticateCallbackHandler> clazz = (Class<? extends AuthenticateCallbackHandler>) configs.get(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS);
        if (clazz == null)
            clazz = clientCallbackHandlerClass();
        AuthenticateCallbackHandler callbackHandler = Utils.newInstance(clazz);
        saslCallbackHandlers.put(clientSaslMechanism, callbackHandler);
    }

    private void createServerCallbackHandlers(Map<String, ?> configs) throws ClassNotFoundException {
        for (String mechanism : jaasContexts.keySet()) {
            AuthenticateCallbackHandler callbackHandler;
            String prefix = ListenerName.saslMechanismPrefix(mechanism);
            @SuppressWarnings("unchecked")
            Class<? extends AuthenticateCallbackHandler> clazz =
                    (Class<? extends AuthenticateCallbackHandler>) configs.get(prefix + BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS);
            if (clazz != null)
                callbackHandler = Utils.newInstance(clazz);
            else if (mechanism.equals(PlainSaslServer.PLAIN_MECHANISM))
                callbackHandler = new PlainServerCallbackHandler();
            else if (ScramMechanism.isScram(mechanism))
                callbackHandler = new ScramServerCallbackHandler(credentialCache.cache(mechanism, ScramCredential.class), tokenCache);
            else if (mechanism.equals(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM))
                callbackHandler = new OAuthBearerUnsecuredValidatorCallbackHandler();
            else
                callbackHandler = new SaslServerCallbackHandler();
            saslCallbackHandlers.put(mechanism, callbackHandler);
        }
    }

    private void createConnectionsMaxReauthMsMap(Map<String, ?> configs) {
        for (String mechanism : jaasContexts.keySet()) {
            String prefix = ListenerName.saslMechanismPrefix(mechanism);
            Long connectionsMaxReauthMs = (Long) configs.get(prefix + BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS);
            if (connectionsMaxReauthMs == null)
                connectionsMaxReauthMs = (Long) configs.get(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS);
            if (connectionsMaxReauthMs != null)
                connectionsMaxReauthMsByMechanism.put(mechanism, connectionsMaxReauthMs);
        }
    }

    private Class<? extends Login> defaultLoginClass(Map<String, ?> configs) {
        if (jaasContexts.containsKey(SaslConfigs.GSSAPI_MECHANISM))
            return KerberosLogin.class;
        if (OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(clientSaslMechanism))
            return OAuthBearerRefreshingLogin.class;
        return DefaultLogin.class;
    }

    private Class<? extends AuthenticateCallbackHandler> clientCallbackHandlerClass() {
        switch (clientSaslMechanism) {
            case SaslConfigs.GSSAPI_MECHANISM:
                return KerberosClientCallbackHandler.class;
            case OAuthBearerLoginModule.OAUTHBEARER_MECHANISM:
                return OAuthBearerSaslClientCallbackHandler.class;
            default:
                return SaslClientCallbackHandler.class;
        }
    }
}
