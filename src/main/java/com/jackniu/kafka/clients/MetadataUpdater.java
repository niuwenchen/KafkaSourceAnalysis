package com.jackniu.kafka.clients;

import com.jackniu.kafka.common.Node;
import com.jackniu.kafka.common.errors.AuthenticationException;
import com.jackniu.kafka.common.requests.MetadataResponse;
import com.jackniu.kafka.common.requests.RequestHeader;

import java.io.Closeable;
import java.util.List;

public interface MetadataUpdater extends Closeable {

    /**
     * Gets the current cluster info without blocking.
     */
    List<Node> fetchNodes();

    /**
     * Returns true if an update to the cluster metadata info is due.
     */
    boolean isUpdateDue(long now);

    /**
     * Starts a cluster metadata update if needed and possible. Returns the time until the metadata update (which would
     * be 0 if an update has been started as a result of this call).
     *
     * If the implementation relies on `NetworkClient` to send requests, `handleCompletedMetadataResponse` will be
     * invoked after the metadata response is received.
     *
     * The semantics of `needed` and `possible` are implementation-dependent and may take into account a number of
     * factors like node availability, how long since the last metadata update, etc.
     */
    long maybeUpdate(long now);

    /**
     * Handle disconnections for metadata requests.
     *
     * This provides a mechanism for the `MetadataUpdater` implementation to use the NetworkClient instance for its own
     * requests with special handling for disconnections of such requests.
     * @param destination
     */
    void handleDisconnection(String destination);

    /**
     * Handle authentication failure. Propagate the authentication exception if awaiting metadata.
     *
     * @param exception authentication exception from broker
     */
    void handleAuthenticationFailure(AuthenticationException exception);

    /**
     * Handle responses for metadata requests.
     *
     * This provides a mechanism for the `MetadataUpdater` implementation to use the NetworkClient instance for its own
     * requests with special handling for completed receives of such requests.
     */
    void handleCompletedMetadataResponse(RequestHeader requestHeader, long now, MetadataResponse metadataResponse);

    /**
     * Schedules an update of the current cluster metadata info. A subsequent call to `maybeUpdate` would trigger the
     * start of the update if possible (see `maybeUpdate` for more information).
     */
    void requestUpdate();

    /**
     * Close this updater.
     */
    @Override
    void close();
}

