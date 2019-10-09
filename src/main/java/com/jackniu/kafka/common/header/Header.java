package com.jackniu.kafka.common.header;

public interface Header {
    String key();
    byte[] value();
}
