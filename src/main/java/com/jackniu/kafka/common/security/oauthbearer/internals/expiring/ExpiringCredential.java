package com.jackniu.kafka.common.security.oauthbearer.internals.expiring;

public interface ExpiringCredential {
    /**
     * The name of the principal to which this credential applies (used only for
     * logging)
     *
     * @return the always non-null/non-empty principal name
     */
    String principalName();

    /**
     * When the credential became valid, in terms of the number of milliseconds
     * since the epoch, if known, otherwise null. An expiring credential may not
     * necessarily indicate when it was created -- just when it expires -- so we
     * need to support a null return value here.
     *
     * @return the time when the credential became valid, in terms of the number of
     *         milliseconds since the epoch, if known, otherwise null
     */
    Long startTimeMs();

    /**
     * When the credential expires, in terms of the number of milliseconds since the
     * epoch. All expiring credentials by definition must indicate their expiration
     * time -- thus, unlike other methods, we do not support a null return value
     * here.
     *
     * @return the time when the credential expires, in terms of the number of
     *         milliseconds since the epoch
     */
    long expireTimeMs();

    /**
     * The point after which the credential can no longer be refreshed, in terms of
     * the number of milliseconds since the epoch, if any, otherwise null. Some
     * expiring credentials can be refreshed over and over again without limit, so
     * we support a null return value here.
     *
     * @return the point after which the credential can no longer be refreshed, in
     *         terms of the number of milliseconds since the epoch, if any,
     *         otherwise null
     */
    Long absoluteLastRefreshTimeMs();

}
