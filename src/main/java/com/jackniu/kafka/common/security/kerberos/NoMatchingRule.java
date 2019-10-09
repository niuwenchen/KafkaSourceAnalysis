package com.jackniu.kafka.common.security.kerberos;

import java.io.IOException;

public class NoMatchingRule  extends IOException {
    NoMatchingRule(String msg) {
        super(msg);
    }
}

