package com.jackniu.kafka.clients;

public interface RequestCompletionHandler {
    public void onComplete(ClientResponse response);
}
