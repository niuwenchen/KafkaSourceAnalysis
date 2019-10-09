package com.jackniu.kafka.common.utils;

import java.io.Closeable;
import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, Closeable {
    void close();
}
