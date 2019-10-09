package com.jackniu.kafka.common.requests;

public class RequestAndSize  {
    public final AbstractRequest request;
    public final int size;

    public RequestAndSize(AbstractRequest request, int size) {
        this.request = request;
        this.size = size;
    }

}
