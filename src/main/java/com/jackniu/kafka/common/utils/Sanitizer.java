package com.jackniu.kafka.common.utils;

import com.jackniu.kafka.common.KafkaException;

import javax.management.ObjectName;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public class Sanitizer  {
    private static final Pattern MBEAN_PATTERN = Pattern.compile("[\\w-%\\. \t]*");

    /**
     * Sanitize `name` for safe use as JMX metric name as well as ZooKeeper node name
     * using URL-encoding.
     */
    public static String sanitize(String name) {
        String encoded = "";
        try {
            encoded = URLEncoder.encode(name, StandardCharsets.UTF_8.name());
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < encoded.length(); i++) {
                char c = encoded.charAt(i);
                if (c == '*') {         // Metric ObjectName treats * as pattern
                    builder.append("%2A");
                } else if (c == '+') {  // Space URL-encoded as +, replace with percent encoding
                    builder.append("%20");
                } else {
                    builder.append(c);
                }
            }
            return builder.toString();
        } catch (UnsupportedEncodingException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Desanitize name that was URL-encoded using {@link #sanitize(String)}. This
     * is used to obtain the desanitized version of node names in ZooKeeper.
     */
    public static String desanitize(String name) {
        try {
            return URLDecoder.decode(name, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Quote `name` using {@link ObjectName#quote(String)} if `name` contains
     * characters that are not safe for use in JMX. User principals that are
     * already sanitized using {@link #sanitize(String)} will not be quoted
     * since they are safe for JMX.
     */
    public static String jmxSanitize(String name) {
        return MBEAN_PATTERN.matcher(name).matches() ? name : ObjectName.quote(name);
    }
}
