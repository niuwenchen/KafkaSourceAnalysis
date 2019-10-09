package com.jackniu.kafka.common.security.scram.internals;


import com.jackniu.kafka.common.security.authenticator.CredentialCache;
import com.jackniu.kafka.common.security.scram.ScramCredential;

import java.util.Base64;
import java.util.Collection;
import java.util.Properties;

public final class ScramCredentialUtils  {
    private static final String SALT = "salt";
    private static final String STORED_KEY = "stored_key";
    private static final String SERVER_KEY = "server_key";
    private static final String ITERATIONS = "iterations";

    private ScramCredentialUtils() {}

    public static String credentialToString(ScramCredential credential) {
        return String.format("%s=%s,%s=%s,%s=%s,%s=%d",
                SALT,
                Base64.getEncoder().encodeToString(credential.salt()),
                STORED_KEY,
                Base64.getEncoder().encodeToString(credential.storedKey()),
                SERVER_KEY,
                Base64.getEncoder().encodeToString(credential.serverKey()),
                ITERATIONS,
                credential.iterations());
    }

    public static ScramCredential credentialFromString(String str) {
        Properties props = toProps(str);
        if (props.size() != 4 || !props.containsKey(SALT) || !props.containsKey(STORED_KEY) ||
                !props.containsKey(SERVER_KEY) || !props.containsKey(ITERATIONS)) {
            throw new IllegalArgumentException("Credentials not valid: " + str);
        }
        byte[] salt = Base64.getDecoder().decode(props.getProperty(SALT));
        byte[] storedKey = Base64.getDecoder().decode(props.getProperty(STORED_KEY));
        byte[] serverKey = Base64.getDecoder().decode(props.getProperty(SERVER_KEY));
        int iterations = Integer.parseInt(props.getProperty(ITERATIONS));
        return new ScramCredential(salt, storedKey, serverKey, iterations);
    }

    private static Properties toProps(String str) {
        Properties props = new Properties();
        String[] tokens = str.split(",");
        for (String token : tokens) {
            int index = token.indexOf('=');
            if (index <= 0)
                throw new IllegalArgumentException("Credentials not valid: " + str);
            props.put(token.substring(0, index), token.substring(index + 1));
        }
        return props;
    }

    public static void createCache(CredentialCache cache, Collection<String> mechanisms) {
        for (String mechanism : ScramMechanism.mechanismNames()) {
            if (mechanisms.contains(mechanism))
                cache.createCache(mechanism, ScramCredential.class);
        }
    }
}
