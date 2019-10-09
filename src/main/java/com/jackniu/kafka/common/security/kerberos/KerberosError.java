package com.jackniu.kafka.common.security.kerberos;

import com.jackniu.kafka.common.KafkaException;
import com.jackniu.kafka.common.security.authenticator.SaslClientAuthenticator;
import com.jackniu.kafka.common.utils.Java;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

public enum KerberosError {
    // (Mechanism level: Server not found in Kerberos database (7) - UNKNOWN_SERVER)
    // This is retriable, but included here to add extra logging for this case.
    SERVER_NOT_FOUND(7, false),
    // (Mechanism level: Client not yet valid - try again later (21))
    CLIENT_NOT_YET_VALID(21, true),
    // (Mechanism level: Ticket not yet valid (33) - Ticket not yet valid)])
    // This could be a small timing window.
    TICKET_NOT_YET_VALID(33, true),
    // (Mechanism level: Request is a replay (34) - Request is a replay)
    // Replay detection used to prevent DoS attacks can result in false positives, so retry on error.
    REPLAY(34, true);

    private static final Logger log = LoggerFactory.getLogger(SaslClientAuthenticator.class);
    private static final Class<?> KRB_EXCEPTION_CLASS;
    private static final Method KRB_EXCEPTION_RETURN_CODE_METHOD;

    static {
        try {
            if (Java.isIbmJdk()) {
                KRB_EXCEPTION_CLASS = Class.forName("com.ibm.security.krb5.internal.KrbException");
            } else {
                KRB_EXCEPTION_CLASS = Class.forName("sun.security.krb5.KrbException");
            }
            KRB_EXCEPTION_RETURN_CODE_METHOD = KRB_EXCEPTION_CLASS.getMethod("returnCode");
        } catch (Exception e) {
            throw new KafkaException("Kerberos exceptions could not be initialized", e);
        }
    }

    private final int errorCode;
    private final boolean retriable;

    KerberosError(int errorCode, boolean retriable) {
        this.errorCode = errorCode;
        this.retriable = retriable;
    }

    public boolean retriable() {
        return retriable;
    }

    public static KerberosError fromException(Exception exception) {
        Throwable cause = exception.getCause();
        while (cause != null && !KRB_EXCEPTION_CLASS.isInstance(cause)) {
            cause = cause.getCause();
        }
        if (cause == null)
            return null;
        else {
            try {
                Integer errorCode = (Integer) KRB_EXCEPTION_RETURN_CODE_METHOD.invoke(cause);
                return fromErrorCode(errorCode);
            } catch (Exception e) {
                log.trace("Kerberos return code could not be determined from {} due to {}", exception, e);
                return null;
            }
        }
    }

    private static KerberosError fromErrorCode(int errorCode) {
        for (KerberosError error : values()) {
            if (error.errorCode == errorCode)
                return error;
        }
        return null;
    }
}
