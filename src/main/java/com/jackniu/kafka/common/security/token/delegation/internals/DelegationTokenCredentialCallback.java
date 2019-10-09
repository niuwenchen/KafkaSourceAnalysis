package com.jackniu.kafka.common.security.token.delegation.internals;

import com.jackniu.kafka.common.security.scram.ScramCredentialCallback;

public class DelegationTokenCredentialCallback  extends ScramCredentialCallback {
    private String tokenOwner;
    private Long tokenExpiryTimestamp;

    public void tokenOwner(String tokenOwner) {
        this.tokenOwner = tokenOwner;
    }

    public String tokenOwner() {
        return tokenOwner;
    }

    public void tokenExpiryTimestamp(Long tokenExpiryTimestamp) {
        this.tokenExpiryTimestamp = tokenExpiryTimestamp;
    }

    public Long tokenExpiryTimestamp() {
        return tokenExpiryTimestamp;
    }
}

