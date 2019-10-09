package com.jackniu.kafka.common.security.token.delegation;

import java.util.Arrays;
import java.util.Base64;

public class DelegationToken  {
    private TokenInformation tokenInformation;
    private byte[] hmac;

    public DelegationToken(TokenInformation tokenInformation, byte[] hmac) {
        this.tokenInformation = tokenInformation;
        this.hmac = hmac;
    }

    public TokenInformation tokenInfo() {
        return tokenInformation;
    }

    public byte[] hmac() {
        return hmac;
    }

    public String hmacAsBase64String() {
        return Base64.getEncoder().encodeToString(hmac);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DelegationToken token = (DelegationToken) o;

        if (tokenInformation != null ? !tokenInformation.equals(token.tokenInformation) : token.tokenInformation != null) {
            return false;
        }
        return Arrays.equals(hmac, token.hmac);
    }

    @Override
    public int hashCode() {
        int result = tokenInformation != null ? tokenInformation.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(hmac);
        return result;
    }

    @Override
    public String toString() {
        return "DelegationToken{" +
                "tokenInformation=" + tokenInformation +
                ", hmac=[*******]" +
                '}';
    }
}
