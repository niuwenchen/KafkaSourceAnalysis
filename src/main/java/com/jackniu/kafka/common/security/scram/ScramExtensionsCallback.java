package com.jackniu.kafka.common.security.scram;

import javax.security.auth.callback.Callback;
import java.util.Collections;
import java.util.Map;

public class ScramExtensionsCallback implements Callback {
    private Map<String, String> extensions = Collections.emptyMap();

    /**
     * Returns map of the extension names and values that are sent by the client to
     * the server in the initial client SCRAM authentication message.
     * Default is an empty unmodifiable map.
     */
    public Map<String, String> extensions() {
        return extensions;
    }

    /**
     * Sets the SCRAM extensions on this callback. Maps passed in should be unmodifiable
     */
    public void extensions(Map<String, String> extensions) {
        this.extensions = extensions;
    }
}

