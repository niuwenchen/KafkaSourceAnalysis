package com.jackniu.kafka.common.network;

import com.jackniu.kafka.common.errors.AuthenticationException;

import java.io.IOException;
import java.nio.channels.*;
import java.security.Principal;

public interface TransportLayer extends ScatteringByteChannel, GatheringByteChannel
{
    /**
     * Returns true if the channel has handshake and authentication done.
     */
    boolean ready();

    /**
     * Finishes the process of connecting a socket channel.
     */
    boolean finishConnect() throws IOException;

    /**
     * disconnect socketChannel
     */
    void disconnect();

    /**
     * Tells whether or not this channel's network socket is connected.
     */
    boolean isConnected();

    /**
     * returns underlying socketChannel
     */
    SocketChannel socketChannel();

    /**
     * Get the underlying selection key
     */
    SelectionKey selectionKey();

    /**
     * This a no-op for the non-secure PLAINTEXT implementation. For SSL, this performs
     * SSL handshake. The SSL handshake includes client authentication if configured using
     * {@link }.
     * @throws  if handshake fails due to an {@link javax.net.ssl.SSLException}.
     * @throws IOException if read or write fails with an I/O error.
     */
    void handshake() throws AuthenticationException, IOException;

    /**
     * Returns true if there are any pending writes
     */
    boolean hasPendingWrites();

    /**
     * Returns `SSLSession.getPeerPrincipal()` if this is a SslTransportLayer and there is an authenticated peer,
     * `KafkaPrincipal.ANONYMOUS` is returned otherwise.
     */
    Principal peerPrincipal() throws IOException;

    void addInterestOps(int ops);

    void removeInterestOps(int ops);

    boolean isMute();

    /**
     * @return true if channel has bytes to be read in any intermediate buffers
     * which may be processed without reading additional data from the network.
     */
    boolean hasBytesBuffered();

    /**
     * Transfers bytes from `fileChannel` to this `TransportLayer`.
     *
     * This method will delegate to {@link FileChannel#transferTo(long, long, java.nio.channels.WritableByteChannel)},
     * but it will unwrap the destination channel, if possible, in order to benefit from zero copy. This is required
     * because the fast path of `transferTo` is only executed if the destination buffer inherits from an internal JDK
     * class.
     *
     * @param fileChannel The source channel
     * @param position The position within the file at which the transfer is to begin; must be non-negative
     * @param count The maximum number of bytes to be transferred; must be non-negative
     * @return The number of bytes, possibly zero, that were actually transferred
     * @see FileChannel#transferTo(long, long, java.nio.channels.WritableByteChannel)
     */
    long transferFrom(FileChannel fileChannel, long position, long count) throws IOException;

}
