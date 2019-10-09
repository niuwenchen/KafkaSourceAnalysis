package com.jackniu.kafka.common.security.scram;

public class ScramCredential {
    private final byte[] salt;
    private final byte[] serverKey;
    private final byte[] storedKey;
    private final int iterations;

    /**
     * Constructs a new credential.
     */
    public ScramCredential(byte[] salt, byte[] storedKey, byte[] serverKey, int iterations) {
        this.salt = salt;
        this.serverKey = serverKey;
        this.storedKey = storedKey;
        this.iterations = iterations;
    }

    /**
     * Returns the salt used to process this credential using the SCRAM algorithm.
     */
    public byte[] salt() {
        return salt;
    }

    /**
     * Server key computed from the client password using the SCRAM algorithm.
     */
    public byte[] serverKey() {
        return serverKey;
    }

    /**
     * Stored key computed from the client password using the SCRAM algorithm.
     */
    public byte[] storedKey() {
        return storedKey;
    }

    /**
     * Number of iterations used to process this credential using the SCRAM algorithm.
     */
    public int iterations() {
        return iterations;
    }

}
