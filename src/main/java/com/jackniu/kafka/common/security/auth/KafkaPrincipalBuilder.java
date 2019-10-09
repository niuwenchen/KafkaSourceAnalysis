package com.jackniu.kafka.common.security.auth;

public interface KafkaPrincipalBuilder {
    /**
     * Build a kafka principal from the authentication context.
     * @param context The authentication context (either {@link SslAuthenticationContext} or
     *        {@link SaslAuthenticationContext})
     * @return The built principal which may provide additional enrichment through a subclass of
     *        {@link KafkaPrincipalBuilder}.
     */
    KafkaPrincipal build(AuthenticationContext context);
}
