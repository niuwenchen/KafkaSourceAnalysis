package com.jackniu.kafka.common.security.auth;

import com.jackniu.kafka.common.security.AuthenticateCallbackHandler;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.util.Map;

public interface Login {
    /**
     * Configures this login instance.
     * @param configs Key-value pairs containing the parsed configuration options of
     *        the client or broker. Note that these are the Kafka configuration options
     *        and not the JAAS configuration options. The JAAS options may be obtained
     *        from `jaasConfiguration`.
     * @param contextName JAAS context name for this login which may be used to obtain
     *        the login context from `jaasConfiguration`.
     * @param jaasConfiguration JAAS configuration containing the login context named
     *        `contextName`. If static JAAS configuration is used, this `Configuration`
     *         may also contain other login contexts.
     * @param loginCallbackHandler Login callback handler instance to use for this Login.
     *        Login callback handler class may be configured using
     *        {@link }.
     */
    void configure(Map<String, ?> configs, String contextName, Configuration jaasConfiguration,
                   AuthenticateCallbackHandler loginCallbackHandler);

    /**
     * Performs login for each login module specified for the login context of this instance.
     */
    LoginContext login() throws LoginException;

    /**
     * Returns the authenticated subject of this login context.
     */
    Subject subject();

    /**
     * Returns the service name to be used for SASL.
     */
    String serviceName();

    /**
     * Closes this instance.
     */
    void close();
}
