package com.jackniu.kafka.common.network;

import java.nio.channels.GatheringByteChannel;

public final class TransportLayers {
    private TransportLayers() {}

    // This is temporary workaround as Send and Receive interfaces are used by BlockingChannel.
    // Once BlockingChannel is removed we can make Send and Receive work with TransportLayer rather than
    // GatheringByteChannel or ScatteringByteChannel.
    public static boolean hasPendingWrites(GatheringByteChannel channel) {
        if (channel instanceof TransportLayer)
            return ((TransportLayer) channel).hasPendingWrites();
        return false;
    }

}
