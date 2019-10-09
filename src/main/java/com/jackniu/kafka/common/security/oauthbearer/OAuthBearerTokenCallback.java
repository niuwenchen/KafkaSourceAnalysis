package com.jackniu.kafka.common.security.oauthbearer;

import javax.security.auth.callback.Callback;
import java.util.Objects;

/**
 * A {@code Callback} for use by the {@code SaslClient} and {@code Login}
 * implementations when they require an OAuth 2 bearer token. Callback handlers
 * should use the {@link #error(String, String, String)} method to communicate
 * errors returned by the authorization server as per
 * <a href="https://tools.ietf.org/html/rfc6749#section-5.2">RFC 6749: The OAuth
 * 2.0 Authorization Framework</a>. Callback handlers should communicate other
 * problems by raising an {@code IOException}.
 * <p>
 * This class was introduced in 2.0.0 and, while it feels stable, it could
 * evolve. We will try to evolve the API in a compatible manner, but we reserve
 * the right to make breaking changes in minor releases, if necessary. We will
 * update the {@code InterfaceStability} annotation and this notice once the API
 * is considered stable.
 */

public class OAuthBearerTokenCallback implements Callback {
    private OAuthBearerToken token = null;
    private String errorCode = null;
    private String errorDescription = null;
    private String errorUri = null;

    /**
     * Return the (potentially null) token
     *
     * @return the (potentially null) token
     */
    public OAuthBearerToken token() {
        return token;
    }

    /**
     * Return the optional (but always non-empty if not null) error code as per
     * <a href="https://tools.ietf.org/html/rfc6749#section-5.2">RFC 6749: The OAuth
     * 2.0 Authorization Framework</a>.
     *
     * @return the optional (but always non-empty if not null) error code
     */
    public String errorCode() {
        return errorCode;
    }

    /**
     * Return the (potentially null) error description as per
     * <a href="https://tools.ietf.org/html/rfc6749#section-5.2">RFC 6749: The OAuth
     * 2.0 Authorization Framework</a>.
     *
     * @return the (potentially null) error description
     */
    public String errorDescription() {
        return errorDescription;
    }

    /**
     * Return the (potentially null) error URI as per
     * <a href="https://tools.ietf.org/html/rfc6749#section-5.2">RFC 6749: The OAuth
     * 2.0 Authorization Framework</a>.
     *
     * @return the (potentially null) error URI
     */
    public String errorUri() {
        return errorUri;
    }

    /**
     * Set the token. All error-related values are cleared.
     *
     * @param token
     *            the optional token to set
     */
    public void token(OAuthBearerToken token) {
        this.token = token;
        this.errorCode = null;
        this.errorDescription = null;
        this.errorUri = null;
    }

    /**
     * Set the error values as per
     * <a href="https://tools.ietf.org/html/rfc6749#section-5.2">RFC 6749: The OAuth
     * 2.0 Authorization Framework</a>. Any token is cleared.
     *
     * @param errorCode
     *            the mandatory error code to set
     * @param errorDescription
     *            the optional error description to set
     * @param errorUri
     *            the optional error URI to set
     */
    public void error(String errorCode, String errorDescription, String errorUri) {
        if (Objects.requireNonNull(errorCode).isEmpty())
            throw new IllegalArgumentException("error code must not be empty");
        this.errorCode = errorCode;
        this.errorDescription = errorDescription;
        this.errorUri = errorUri;
        this.token = null;
    }
}

