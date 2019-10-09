package com.jackniu.kafka.common.errors;

public class GroupAuthorizationException extends AuthorizationException {
    private final String groupId;

    public GroupAuthorizationException(String groupId) {
        super("Not authorized to access group: " + groupId);
        this.groupId = groupId;
    }

    public String groupId() {
        return groupId;
    }

}
