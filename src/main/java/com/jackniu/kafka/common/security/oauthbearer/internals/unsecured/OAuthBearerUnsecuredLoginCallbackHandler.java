package com.jackniu.kafka.common.security.oauthbearer.internals.unsecured;

import com.jackniu.kafka.common.KafkaException;
import com.jackniu.kafka.common.config.ConfigException;
import com.jackniu.kafka.common.security.AuthenticateCallbackHandler;
import com.jackniu.kafka.common.security.auth.SaslExtensions;
import com.jackniu.kafka.common.security.auth.SaslExtensionsCallback;
import com.jackniu.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import com.jackniu.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import com.jackniu.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;
import com.jackniu.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OAuthBearerUnsecuredLoginCallbackHandler implements AuthenticateCallbackHandler {
    private final Logger log = LoggerFactory.getLogger(OAuthBearerUnsecuredLoginCallbackHandler.class);
    private static final String OPTION_PREFIX = "unsecuredLogin";
    private static final String PRINCIPAL_CLAIM_NAME_OPTION = OPTION_PREFIX + "PrincipalClaimName";
    private static final String LIFETIME_SECONDS_OPTION = OPTION_PREFIX + "LifetimeSeconds";
    private static final String SCOPE_CLAIM_NAME_OPTION = OPTION_PREFIX + "ScopeClaimName";
    private static final Set<String> RESERVED_CLAIMS = Collections
            .unmodifiableSet(new HashSet<>(Arrays.asList("iat", "exp")));
    private static final String DEFAULT_PRINCIPAL_CLAIM_NAME = "sub";
    private static final String DEFAULT_LIFETIME_SECONDS_ONE_HOUR = "3600";
    private static final String DEFAULT_SCOPE_CLAIM_NAME = "scope";
    private static final String STRING_CLAIM_PREFIX = OPTION_PREFIX + "StringClaim_";
    private static final String NUMBER_CLAIM_PREFIX = OPTION_PREFIX + "NumberClaim_";
    private static final String LIST_CLAIM_PREFIX = OPTION_PREFIX + "ListClaim_";
    private static final String EXTENSION_PREFIX = OPTION_PREFIX + "Extension_";
    private static final String QUOTE = "\"";
    private Time time = Time.SYSTEM;
    private Map<String, String> moduleOptions = null;
    private boolean configured = false;

    private static final Pattern DOUBLEQUOTE = Pattern.compile("\"", Pattern.LITERAL);

    private static final Pattern BACKSLASH = Pattern.compile("\\", Pattern.LITERAL);

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
        this.moduleOptions = Collections.unmodifiableMap((Map<String, String>) jaasConfigEntries.get(0).getOptions());
        configured = true;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        if (!configured())
            throw new IllegalStateException("Callback handler not configured");
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback)
                try {
                    handleTokenCallback((OAuthBearerTokenCallback) callback);
                } catch (KafkaException e) {
                    throw new IOException(e.getMessage(), e);
                }
            else if (callback instanceof SaslExtensionsCallback)
                try {
                    handleExtensionsCallback((SaslExtensionsCallback) callback);
                } catch (KafkaException e) {
                    throw new IOException(e.getMessage(), e);
                }
            else
                throw new UnsupportedCallbackException(callback);
        }
    }

    @Override
    public void close() {
        // empty
    }

    private void handleTokenCallback(OAuthBearerTokenCallback callback) {
        if (callback.token() != null)
            throw new IllegalArgumentException("Callback had a token already");
        if (moduleOptions.isEmpty()) {
            log.debug("Token not provided, this login cannot be used to establish client connections");
            callback.token(null);
            return;
        }
        if (moduleOptions.keySet().stream().noneMatch(name -> !name.startsWith(EXTENSION_PREFIX))) {
            throw new OAuthBearerConfigException("Extensions provided in login context without a token");
        }
        String principalClaimNameValue = optionValue(PRINCIPAL_CLAIM_NAME_OPTION);
        String principalClaimName = principalClaimNameValue != null && !principalClaimNameValue.trim().isEmpty()
                ? principalClaimNameValue.trim()
                : DEFAULT_PRINCIPAL_CLAIM_NAME;
        String scopeClaimNameValue = optionValue(SCOPE_CLAIM_NAME_OPTION);
        String scopeClaimName = scopeClaimNameValue != null && !scopeClaimNameValue.trim().isEmpty()
                ? scopeClaimNameValue.trim()
                : DEFAULT_SCOPE_CLAIM_NAME;
        String headerJson = "{" + claimOrHeaderJsonText("alg", "none") + "}";
        String lifetimeSecondsValueToUse = optionValue(LIFETIME_SECONDS_OPTION, DEFAULT_LIFETIME_SECONDS_ONE_HOUR);
        String claimsJson;
        try {
            claimsJson = String.format("{%s,%s%s}", expClaimText(Long.parseLong(lifetimeSecondsValueToUse)),
                    claimOrHeaderJsonText("iat", time.milliseconds() / 1000.0),
                    commaPrependedStringNumberAndListClaimsJsonText());
        } catch (NumberFormatException e) {
            throw new OAuthBearerConfigException(e.getMessage());
        }
        try {
            Base64.Encoder urlEncoderNoPadding = Base64.getUrlEncoder().withoutPadding();
            OAuthBearerUnsecuredJws jws = new OAuthBearerUnsecuredJws(
                    String.format("%s.%s.",
                            urlEncoderNoPadding.encodeToString(headerJson.getBytes(StandardCharsets.UTF_8)),
                            urlEncoderNoPadding.encodeToString(claimsJson.getBytes(StandardCharsets.UTF_8))),
                    principalClaimName, scopeClaimName);
            log.info("Retrieved token with principal {}", jws.principalName());
            callback.token(jws);
        } catch (OAuthBearerIllegalTokenException e) {
            // occurs if the principal claim doesn't exist or has an empty value
            throw new OAuthBearerConfigException(e.getMessage(), e);
        }
    }

    /**
     *  Add and validate all the configured extensions.
     *  Token keys, apart from passing regex validation, must not be equal to the reserved key {@link OAuthBearerClientInitialResponse#AUTH_KEY}
     */
    private void handleExtensionsCallback(SaslExtensionsCallback callback) {
        Map<String, String> extensions = new HashMap<>();
        for (Map.Entry<String, String> configEntry : this.moduleOptions.entrySet()) {
            String key = configEntry.getKey();
            if (!key.startsWith(EXTENSION_PREFIX))
                continue;

            extensions.put(key.substring(EXTENSION_PREFIX.length()), configEntry.getValue());
        }

        SaslExtensions saslExtensions = new SaslExtensions(extensions);
        try {
            OAuthBearerClientInitialResponse.validateExtensions(saslExtensions);
        } catch (SaslException e) {
            throw new ConfigException(e.getMessage());
        }

        callback.extensions(saslExtensions);
    }

    private String commaPrependedStringNumberAndListClaimsJsonText() throws OAuthBearerConfigException {
        StringBuilder sb = new StringBuilder();
        for (String key : moduleOptions.keySet()) {
            if (key.startsWith(STRING_CLAIM_PREFIX) && key.length() > STRING_CLAIM_PREFIX.length())
                sb.append(',').append(claimOrHeaderJsonText(
                        confirmNotReservedClaimName(key.substring(STRING_CLAIM_PREFIX.length())), optionValue(key)));
            else if (key.startsWith(NUMBER_CLAIM_PREFIX) && key.length() > NUMBER_CLAIM_PREFIX.length())
                sb.append(',')
                        .append(claimOrHeaderJsonText(
                                confirmNotReservedClaimName(key.substring(NUMBER_CLAIM_PREFIX.length())),
                                Double.valueOf(optionValue(key))));
            else if (key.startsWith(LIST_CLAIM_PREFIX) && key.length() > LIST_CLAIM_PREFIX.length())
                sb.append(',')
                        .append(claimOrHeaderJsonArrayText(
                                confirmNotReservedClaimName(key.substring(LIST_CLAIM_PREFIX.length())),
                                listJsonText(optionValue(key))));
        }
        return sb.toString();
    }

    private String confirmNotReservedClaimName(String claimName) throws OAuthBearerConfigException {
        if (RESERVED_CLAIMS.contains(claimName))
            throw new OAuthBearerConfigException(String.format("Cannot explicitly set the '%s' claim", claimName));
        return claimName;
    }

    private String listJsonText(String value) {
        if (value.isEmpty() || value.length() <= 1)
            return "[]";
        String delimiter;
        String unescapedDelimiterChar = value.substring(0, 1);
        switch (unescapedDelimiterChar) {
            case "\\":
            case ".":
            case "[":
            case "(":
            case "{":
            case "|":
            case "^":
            case "$":
                delimiter = "\\" + unescapedDelimiterChar;
                break;
            default:
                delimiter = unescapedDelimiterChar;
                break;
        }
        String listText = value.substring(1);
        String[] elements = listText.split(delimiter);
        StringBuilder sb = new StringBuilder();
        for (String element : elements) {
            sb.append(sb.length() == 0 ? '[' : ',');
            sb.append('"').append(escape(element)).append('"');
        }
        if (listText.startsWith(unescapedDelimiterChar) || listText.endsWith(unescapedDelimiterChar)
                || listText.contains(unescapedDelimiterChar + unescapedDelimiterChar))
            sb.append(",\"\"");
        return sb.append(']').toString();
    }

    private String optionValue(String key) {
        return optionValue(key, null);
    }

    private String optionValue(String key, String defaultValue) {
        String explicitValue = option(key);
        return explicitValue != null ? explicitValue : defaultValue;
    }

    private String option(String key) {
        if (!configured)
            throw new IllegalStateException("Callback handler not configured");
        return moduleOptions.get(Objects.requireNonNull(key));
    }

    private String claimOrHeaderJsonText(String claimName, Number claimValue) {
        return QUOTE + escape(claimName) + QUOTE + ":" + claimValue;
    }

    private String claimOrHeaderJsonText(String claimName, String claimValue) {
        return QUOTE + escape(claimName) + QUOTE + ":" + QUOTE + escape(claimValue) + QUOTE;
    }

    private String claimOrHeaderJsonArrayText(String claimName, String escapedClaimValue) {
        if (!escapedClaimValue.startsWith("[") || !escapedClaimValue.endsWith("]"))
            throw new IllegalArgumentException(String.format("Illegal JSON array: %s", escapedClaimValue));
        return QUOTE + escape(claimName) + QUOTE + ":" + escapedClaimValue;
    }

    private String escape(String jsonStringValue) {
        String replace1 = DOUBLEQUOTE.matcher(jsonStringValue).replaceAll(Matcher.quoteReplacement("\\\""));
        return BACKSLASH.matcher(replace1).replaceAll(Matcher.quoteReplacement("\\\\"));
    }

    private String expClaimText(long lifetimeSeconds) {
        return claimOrHeaderJsonText("exp", time.milliseconds() / 1000.0 + lifetimeSeconds);
    }
}

