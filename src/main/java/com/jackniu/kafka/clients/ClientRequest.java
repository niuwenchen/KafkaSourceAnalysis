package com.jackniu.kafka.clients;

import com.jackniu.kafka.common.protocol.ApiKeys;
import com.jackniu.kafka.common.requests.AbstractRequest;
import com.jackniu.kafka.common.requests.RequestHeader;

public final class ClientRequest {
    private final String destination;
    private final AbstractRequest.Builder<?> requestBuilder;
    private final int correlationId;
    private final String clientId;
    private final long createdTimeMs;
    private final boolean expectResponse;
    private final int requestTimeoutMs;
    private final RequestCompletionHandler callback;

    /**
     * @param destination The brokerId to send the request to
     * @param requestBuilder The builder for the request to make
     * @param correlationId The correlation id for this client request
     * @param clientId The client ID to use for the header
     * @param createdTimeMs The unix timestamp in milliseconds for the time at which this request was created.
     * @param expectResponse Should we expect a response message or is this request complete once it is sent?
     * @param callback A callback to execute when the response has been received (or null if no callback is necessary)
     */
    public ClientRequest(String destination,
                         AbstractRequest.Builder<?> requestBuilder,
                         int correlationId,
                         String clientId,
                         long createdTimeMs,
                         boolean expectResponse,
                         int requestTimeoutMs,
                         RequestCompletionHandler callback) {
        this.destination = destination;
        this.requestBuilder = requestBuilder;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.createdTimeMs = createdTimeMs;
        this.expectResponse = expectResponse;
        this.requestTimeoutMs = requestTimeoutMs;
        this.callback = callback;
    }

    @Override
    public String toString() {
        return "ClientRequest(expectResponse=" + expectResponse +
                ", callback=" + callback +
                ", destination=" + destination +
                ", correlationId=" + correlationId +
                ", clientId=" + clientId +
                ", createdTimeMs=" + createdTimeMs +
                ", requestBuilder=" + requestBuilder +
                ")";
    }

    public boolean expectResponse() {
        return expectResponse;
    }

    public ApiKeys apiKey() {
        return requestBuilder.apiKey();
    }

    public RequestHeader makeHeader(short version) {
        return new RequestHeader(apiKey(), version, clientId, correlationId);
    }

    public AbstractRequest.Builder<?> requestBuilder() {
        return requestBuilder;
    }

    public String destination() {
        return destination;
    }

    public RequestCompletionHandler callback() {
        return callback;
    }

    public long createdTimeMs() {
        return createdTimeMs;
    }

    public int correlationId() {
        return correlationId;
    }

    public int requestTimeoutMs() {
        return requestTimeoutMs;
    }
}
