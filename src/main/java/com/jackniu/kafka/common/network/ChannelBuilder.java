package com.jackniu.kafka.common.network;

import com.jackniu.kafka.common.Configurable;
import com.jackniu.kafka.common.KafkaException;
import com.jackniu.kafka.common.memory.MemoryPool;

import java.nio.channels.SelectionKey;

public interface ChannelBuilder extends AutoCloseable, Configurable {

    /**
     * returns a Channel with TransportLayer and Authenticator configured.
     * @param  id  channel id
     * @param  key SelectionKey
     * @param  maxReceiveSize max size of a single receive buffer to allocate
     * @param  memoryPool memory pool from which to allocate buffers, or null for none
     * @return KafkaChannel
     */
    KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize, MemoryPool memoryPool) throws KafkaException;

    /**
     * Closes ChannelBuilder
     */
    @Override
    void close();

}

