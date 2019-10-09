package com.jackniu.kafka.common.security.kerberos;

import java.io.IOException;

public class BadFormatString  extends IOException {
    BadFormatString(String msg) {
        super(msg);
    }
    BadFormatString(String msg, Throwable err) {
        super(msg, err);
    }
}
