package com.jackniu.kafka.common.security;

import com.jackniu.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;

public final class JaasUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JaasUtils.class);
    public static final String JAVA_LOGIN_CONFIG_PARAM = "java.security.auth.login.config";

    public static final String SERVICE_NAME = "serviceName";

    public static final String ZK_SASL_CLIENT = "zookeeper.sasl.client";
    public static final String ZK_LOGIN_CONTEXT_NAME_KEY = "zookeeper.sasl.clientconfig";

    private JaasUtils() {}

    public static boolean isZkSecurityEnabled() {
        boolean zkSaslEnabled = Boolean.parseBoolean(System.getProperty(ZK_SASL_CLIENT, "true"));
        String zkLoginContextName = System.getProperty(ZK_LOGIN_CONTEXT_NAME_KEY, "Client");

        boolean isSecurityEnabled;
        try {
            Configuration loginConf = Configuration.getConfiguration();
            isSecurityEnabled = loginConf.getAppConfigurationEntry(zkLoginContextName) != null;
        } catch (Exception e) {
            throw new KafkaException("Exception while loading Zookeeper JAAS login context '" + zkLoginContextName + "'", e);
        }
        if (isSecurityEnabled && !zkSaslEnabled) {
            LOG.error("JAAS configuration is present, but system property " +
                    ZK_SASL_CLIENT + " is set to false, which disables " +
                    "SASL in the ZooKeeper client");
            throw new KafkaException("Exception while determining if ZooKeeper is secure");
        }

        return isSecurityEnabled;
    }
}

