package com.jackniu.kafka.common.security.authenticator;

import java.util.concurrent.ConcurrentHashMap;

public class CredentialCache {
    private final ConcurrentHashMap<String, Cache<? extends Object>> cacheMap = new ConcurrentHashMap<>();

    public <C> Cache<C> createCache(String mechanism, Class<C> credentialClass) {
        Cache<C> cache = new Cache<>(credentialClass);
        @SuppressWarnings("unchecked")
        Cache<C> oldCache = (Cache<C>) cacheMap.putIfAbsent(mechanism, cache);
        return oldCache == null ? cache : oldCache;
    }

    @SuppressWarnings("unchecked")
    public <C> Cache<C> cache(String mechanism, Class<C> credentialClass) {
        Cache<?> cache = cacheMap.get(mechanism);
        if (cache != null) {
            if (cache.credentialClass() != credentialClass)
                throw new IllegalArgumentException("Invalid credential class " + credentialClass + ", expected " + cache.credentialClass());
            return (Cache<C>) cache;
        } else
            return null;
    }

    public static class Cache<C> {
        private final Class<C> credentialClass;
        private final ConcurrentHashMap<String, C> credentials;

        public Cache(Class<C> credentialClass) {
            this.credentialClass = credentialClass;
            this.credentials = new ConcurrentHashMap<>();
        }

        public C get(String username) {
            return credentials.get(username);
        }

        public C put(String username, C credential) {
            return credentials.put(username, credential);
        }

        public C remove(String username) {
            return credentials.remove(username);
        }

        public Class<C> credentialClass() {
            return credentialClass;
        }
    }
}
