package com.jackniu.kafka.common.security.scram;


import javax.security.auth.callback.Callback;

public class ScramCredentialCallback implements Callback {
    private ScramCredential scramCredential;

    /**
     * Sets the SCRAM credential for this instance.
     */
    public void scramCredential(ScramCredential scramCredential) {
        this.scramCredential = scramCredential;
    }

    /**
     * Returns the SCRAM credential if set on this instance.
     */
    public ScramCredential scramCredential() {
        return scramCredential;
    }


}
