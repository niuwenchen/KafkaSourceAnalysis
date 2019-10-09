package com.jackniu.kafka.common.errors;

public class GroupMaxSizeReachedException  extends ApiException {
    private static final long serialVersionUID = 1L;

    public GroupMaxSizeReachedException(String groupId) {
        super("Consumer group " + groupId + " already has the configured maximum number of members.");
    }
}
