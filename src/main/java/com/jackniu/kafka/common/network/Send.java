package com.jackniu.kafka.common.network;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

public interface Send {
    /**
     * The id for the destination of this send
     */
    String destination();

    /**
     * Is this send complete?
     */
    boolean completed();

    long writeTo(GatheringByteChannel channel) throws IOException;

    /**
     * Size of the send
     */
    long size();

}
