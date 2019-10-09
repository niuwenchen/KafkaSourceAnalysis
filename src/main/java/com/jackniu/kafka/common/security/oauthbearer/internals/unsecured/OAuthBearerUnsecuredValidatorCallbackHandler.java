package com.jackniu.kafka.common.security.oauthbearer.internals.unsecured;

import com.jackniu.kafka.common.security.AuthenticateCallbackHandler;
import com.jackniu.kafka.common.security.oauthbearer.OAuthBearerExtensionsValidatorCallback;
import com.jackniu.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import com.jackniu.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import com.jackniu.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OAuthBearerUnsecuredValidatorCallbackHandler implements AuthenticateCallbackHandler {
    private static final Logger log = LoggerFactory.getLogger(OAuthBearerUnsecuredValidatorCallbackHandler.class);
    private static final String OPTION_PREFIX = "unsecuredValidator";
    private static final String PRINCIPAL_CLAIM_NAME_OPTION = OPTION_PREFIX + "PrincipalClaimName";
    private static final String SCOPE_CLAIM_NAME_OPTION = OPTION_PREFIX + "ScopeClaimName";
    private static final String REQUIRED_SCOPE_OPTION = OPTION_PREFIX + "RequiredScope";
    private static final String ALLOWABLE_CLOCK_SKEW_MILLIS_OPTION = OPTION_PREFIX + "AllowableClockSkewMs";
    private Time time = Time.SYSTEM;
    private Map<String, String> moduleOptions = null;
    private boolean configured = false;

    /**
     * For testing
     *
     * @param time
     *            the mandatory time to set
     */
    void time(Time time) {
        this.time = Objects.requireNonNull(time);
    }

    /**
     * Return true if this instance has been configured, otherwise false
     *
     * @return true if this instance has been configured, otherwise false
     */
    public boolean configured() {
        return configured;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism))
            throw new IllegalArgumentException(String.format("Unexpected SASL mechanism: %s", saslMechanism));
        if (Objects.requireNonNull(jaasConfigEntries).size() != 1 || jaasConfigEntries.get(0) == null)
            throw new IllegalArgumentException(
                    String.format("Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
                            jaasConfigEntries.size()));
        final Map<String, String> unmodifiableModuleOptions = Collections
                .unmodifiableMap((Map<String, String>) jaasConfigEntries.get(0).getOptions());
        this.moduleOptions = unmodifiableModuleOptions;
        configured = true;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        if (!configured())
            throw new IllegalStateException("Callback handler not configured");
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerValidatorCallback) {
                OAuthBearerValidatorCallback validationCallback = (OAuthBearerValidatorCallback) callback;
                try {
                    handleCallback(validationCallback);
                } catch (OAuthBearerIllegalTokenException e) {
                    OAuthBearerValidationResult failureReason = e.reason();
                    String failureScope = failureReason.failureScope();
                    validationCallback.error(failureScope != null ? "insufficient_scope" : "invalid_token",
                            failureScope, failureReason.failureOpenIdConfig());
                }
            } else if (callback instanceof OAuthBearerExtensionsValidatorCallback) {
                OAuthBearerExtensionsValidatorCallback extensionsCallback = (OAuthBearerExtensionsValidatorCallback) callback;
                extensionsCallback.inputExtensions().map().forEach((extensionName, v) -> extensionsCallback.valid(extensionName));
            } else
                throw new UnsupportedCallbackException(callback);
        }
    }

    @Override
    public void close() {
        // empty
    }

    private void handleCallback(OAuthBearerValidatorCallback callback) {
        String tokenValue = callback.tokenValue();
        if (tokenValue == null)
            throw new IllegalArgumentException("Callback missing required token value");
        String principalClaimName = principalClaimName();
        String scopeClaimName = scopeClaimName();
        List<String> requiredScope = requiredScope();
        int allowableClockSkewMs = allowableClockSkewMs();
        OAuthBearerUnsecuredJws unsecuredJwt = new OAuthBearerUnsecuredJws(tokenValue, principalClaimName,
                scopeClaimName);
        long now = time.milliseconds();
        OAuthBearerValidationUtils
                .validateClaimForExistenceAndType(unsecuredJwt, true, principalClaimName, String.class)
                .throwExceptionIfFailed();
        OAuthBearerValidationUtils.validateIssuedAt(unsecuredJwt, false, now, allowableClockSkewMs)
                .throwExceptionIfFailed();
        OAuthBearerValidationUtils.validateExpirationTime(unsecuredJwt, now, allowableClockSkewMs)
                .throwExceptionIfFailed();
        OAuthBearerValidationUtils.validateTimeConsistency(unsecuredJwt).throwExceptionIfFailed();
        OAuthBearerValidationUtils.validateScope(unsecuredJwt, requiredScope).throwExceptionIfFailed();
        log.info("Successfully validated token with principal {}: {}", unsecuredJwt.principalName(),
                unsecuredJwt.claims().toString());
        callback.token(unsecuredJwt);
    }

    private String principalClaimName() {
        String principalClaimNameValue = option(PRINCIPAL_CLAIM_NAME_OPTION);
        String principalClaimName = principalClaimNameValue != null && !principalClaimNameValue.trim().isEmpty()
                ? principalClaimNameValue.trim()
                : "sub";
        return principalClaimName;
    }

    private String scopeClaimName() {
        String scopeClaimNameValue = option(SCOPE_CLAIM_NAME_OPTION);
        String scopeClaimName = scopeClaimNameValue != null && !scopeClaimNameValue.trim().isEmpty()
                ? scopeClaimNameValue.trim()
                : "scope";
        return scopeClaimName;
    }

    private List<String> requiredScope() {
        String requiredSpaceDelimitedScope = option(REQUIRED_SCOPE_OPTION);
        List<String> requiredScope = requiredSpaceDelimitedScope == null || requiredSpaceDelimitedScope.trim().isEmpty()
                ? Collections.emptyList()
                : OAuthBearerScopeUtils.parseScope(requiredSpaceDelimitedScope.trim());
        return requiredScope;
    }

    private int allowableClockSkewMs() {
        String allowableClockSkewMsValue = option(ALLOWABLE_CLOCK_SKEW_MILLIS_OPTION);
        int allowableClockSkewMs = 0;
        try {
            allowableClockSkewMs = allowableClockSkewMsValue == null || allowableClockSkewMsValue.trim().isEmpty() ? 0
                    : Integer.parseInt(allowableClockSkewMsValue.trim());
        } catch (NumberFormatException e) {
            throw new OAuthBearerConfigException(e.getMessage(), e);
        }
        if (allowableClockSkewMs < 0) {
            throw new OAuthBearerConfigException(
                    String.format("Allowable clock skew millis must not be negative: %s", allowableClockSkewMsValue));
        }
        return allowableClockSkewMs;
    }

    private String option(String key) {
        if (!configured)
            throw new IllegalStateException("Callback handler not configured");
        return moduleOptions.get(Objects.requireNonNull(key));
    }
}
