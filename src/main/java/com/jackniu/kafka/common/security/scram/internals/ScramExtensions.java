package com.jackniu.kafka.common.security.scram.internals;

import com.jackniu.kafka.common.security.auth.SaslExtensions;
import com.jackniu.kafka.common.security.scram.ScramLoginModule;
import com.jackniu.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.Map;

public class ScramExtensions extends SaslExtensions {

    public ScramExtensions() {
        this(Collections.emptyMap());
    }

    public ScramExtensions(String extensions) {
        this(Utils.parseMap(extensions, "=", ","));
    }

    public ScramExtensions(Map<String, String> extensionMap) {
        super(extensionMap);
    }

    public boolean tokenAuthenticated() {
        return Boolean.parseBoolean(map().get(ScramLoginModule.TOKEN_AUTH_CONFIG));
    }
}

