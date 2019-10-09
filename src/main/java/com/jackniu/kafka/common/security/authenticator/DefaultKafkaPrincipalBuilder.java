package com.jackniu.kafka.common.security.authenticator;

import com.jackniu.kafka.common.KafkaException;
import com.jackniu.kafka.common.config.SaslConfigs;
import com.jackniu.kafka.common.network.Authenticator;
import com.jackniu.kafka.common.network.TransportLayer;
import com.jackniu.kafka.common.security.auth.*;
import com.jackniu.kafka.common.security.kerberos.KerberosName;
import com.jackniu.kafka.common.security.kerberos.KerberosShortNamer;
import com.jackniu.kafka.common.security.ssl.SslPrincipalMapper;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import javax.security.sasl.SaslServer;
import java.io.Closeable;
import java.io.IOException;
import java.security.Principal;

import static java.util.Objects.requireNonNull;

public class DefaultKafkaPrincipalBuilder implements KafkaPrincipalBuilder, Closeable {
    // Use FQN to avoid import deprecation warnings
    @SuppressWarnings("deprecation")
    private final PrincipalBuilder oldPrincipalBuilder;
    private final Authenticator authenticator;
    private final TransportLayer transportLayer;
    private final KerberosShortNamer kerberosShortNamer;
    private final SslPrincipalMapper sslPrincipalMapper;

    /**
     * Construct a new instance which wraps an instance of the older {@link }
     *
     * @param authenticator The authenticator in use
     * @param transportLayer The underlying transport layer
     * @param oldPrincipalBuilder Instance of {@link }
     * @param kerberosShortNamer Kerberos name rewrite rules or null if none have been configured
     */
    @SuppressWarnings("deprecation")
    public static DefaultKafkaPrincipalBuilder fromOldPrincipalBuilder(Authenticator authenticator,
                                                                       TransportLayer transportLayer,
                                                                       PrincipalBuilder oldPrincipalBuilder,
                                                                       KerberosShortNamer kerberosShortNamer) {
        return new DefaultKafkaPrincipalBuilder(
                requireNonNull(authenticator),
                requireNonNull(transportLayer),
                requireNonNull(oldPrincipalBuilder),
                kerberosShortNamer,
                null);
    }

    @SuppressWarnings("deprecation")
    private DefaultKafkaPrincipalBuilder(Authenticator authenticator,
                                         TransportLayer transportLayer,
                                         PrincipalBuilder oldPrincipalBuilder,
                                         KerberosShortNamer kerberosShortNamer,
                                         SslPrincipalMapper sslPrincipalMapper) {
        this.authenticator = authenticator;
        this.transportLayer = transportLayer;
        this.oldPrincipalBuilder = oldPrincipalBuilder;
        this.kerberosShortNamer = kerberosShortNamer;
        this.sslPrincipalMapper =  sslPrincipalMapper;
    }

    /**
     * Construct a new instance.
     *
     * @param kerberosShortNamer Kerberos name rewrite rules or null if none have been configured
     * @param sslPrincipalMapper SSL Principal mapper or null if none have been configured
     */
    public DefaultKafkaPrincipalBuilder(KerberosShortNamer kerberosShortNamer, SslPrincipalMapper sslPrincipalMapper) {
        this(null, null, null, kerberosShortNamer, sslPrincipalMapper);
    }

    @Override
    public KafkaPrincipal build(AuthenticationContext context) {
        if (context instanceof PlaintextAuthenticationContext) {
            if (oldPrincipalBuilder != null)
                return convertToKafkaPrincipal(oldPrincipalBuilder.buildPrincipal(transportLayer, authenticator));

            return KafkaPrincipal.ANONYMOUS;
        } else if (context instanceof SslAuthenticationContext) {
            SSLSession sslSession = ((SslAuthenticationContext) context).session();

            if (oldPrincipalBuilder != null)
                return convertToKafkaPrincipal(oldPrincipalBuilder.buildPrincipal(transportLayer, authenticator));

            try {
                return applySslPrincipalMapper(sslSession.getPeerPrincipal());
            } catch (SSLPeerUnverifiedException se) {
                return KafkaPrincipal.ANONYMOUS;
            }
        } else if (context instanceof SaslAuthenticationContext) {
            SaslServer saslServer = ((SaslAuthenticationContext) context).server();
            if (SaslConfigs.GSSAPI_MECHANISM.equals(saslServer.getMechanismName()))
                return applyKerberosShortNamer(saslServer.getAuthorizationID());
            else
                return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, saslServer.getAuthorizationID());
        } else {
            throw new IllegalArgumentException("Unhandled authentication context type: " + context.getClass().getName());
        }
    }

    private KafkaPrincipal applyKerberosShortNamer(String authorizationId) {
        KerberosName kerberosName = KerberosName.parse(authorizationId);
        try {
            String shortName = kerberosShortNamer.shortName(kerberosName);
            return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, shortName);
        } catch (IOException e) {
            throw new KafkaException("Failed to set name for '" + kerberosName +
                    "' based on Kerberos authentication rules.", e);
        }
    }

    private KafkaPrincipal applySslPrincipalMapper(Principal principal) {
        try {
            if (!(principal instanceof X500Principal) || principal == KafkaPrincipal.ANONYMOUS) {
                return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principal.getName());
            } else {
                return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, sslPrincipalMapper.getName(principal.getName()));
            }
        } catch (IOException e) {
            throw new KafkaException("Failed to map name for '" + principal.getName() +
                    "' based on SSL principal mapping rules.", e);
        }
    }

    private KafkaPrincipal convertToKafkaPrincipal(Principal principal) {
        return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principal.getName());
    }

    @Override
    public void close() {
        if (oldPrincipalBuilder != null)
            oldPrincipalBuilder.close();
    }

}

