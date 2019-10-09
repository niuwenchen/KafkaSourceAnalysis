package com.jackniu.kafka.clients;

import com.jackniu.kafka.common.errors.AuthenticationException;
import com.jackniu.kafka.common.errors.UnsupportedVersionException;
import com.jackniu.kafka.common.requests.AbstractResponse;
import com.jackniu.kafka.common.requests.RequestHeader;

public class ClientResponse  {
    private final RequestHeader requestHeader;
    private final RequestCompletionHandler callback;
    private final String destination;
    private final long receivedTimeMs;
    private final long latencyMs;
    private final boolean disconnected;
    private final UnsupportedVersionException versionMismatch;
    private final AuthenticationException authenticationException;
    private final AbstractResponse responseBody;

    /**
     * @param requestHeader The header of the corresponding request
     * @param callback The callback to be invoked
     * @param createdTimeMs The unix timestamp when the corresponding request was created
     * @param destination The node the corresponding request was sent to
     * @param receivedTimeMs The unix timestamp when this response was received
     * @param disconnected Whether the client disconnected before fully reading a response
     * @param versionMismatch Whether there was a version mismatch that prevented sending the request.
     * @param responseBody The response contents (or null) if we disconnected, no response was expected,
     *                     or if there was a version mismatch.
     */
    public ClientResponse(RequestHeader requestHeader,
                          RequestCompletionHandler callback,
                          String destination,
                          long createdTimeMs,
                          long receivedTimeMs,
                          boolean disconnected,
                          UnsupportedVersionException versionMismatch,
                          AuthenticationException authenticationException,
                          AbstractResponse responseBody) {
        this.requestHeader = requestHeader;
        this.callback = callback;
        this.destination = destination;
        this.receivedTimeMs = receivedTimeMs;
        this.latencyMs = receivedTimeMs - createdTimeMs;
        this.disconnected = disconnected;
        this.versionMismatch = versionMismatch;
        this.authenticationException = authenticationException;
        this.responseBody = responseBody;
    }

    public long receivedTimeMs() {
        return receivedTimeMs;
    }

    public boolean wasDisconnected() {
        return disconnected;
    }

    public UnsupportedVersionException versionMismatch() {
        return versionMismatch;
    }

    public AuthenticationException authenticationException() {
        return authenticationException;
    }

    public RequestHeader requestHeader() {
        return requestHeader;
    }

    public String destination() {
        return destination;
    }

    public AbstractResponse responseBody() {
        return responseBody;
    }

    public boolean hasResponse() {
        return responseBody != null;
    }

    public long requestLatencyMs() {
        return latencyMs;
    }

    public void onComplete() {
        if (callback != null)
            callback.onComplete(this);
    }

    @Override
    public String toString() {
        return "ClientResponse(receivedTimeMs=" + receivedTimeMs +
                ", latencyMs=" +
                latencyMs +
                ", disconnected=" +
                disconnected +
                ", requestHeader=" +
                requestHeader +
                ", responseBody=" +
                responseBody +
                ")";
    }
}
