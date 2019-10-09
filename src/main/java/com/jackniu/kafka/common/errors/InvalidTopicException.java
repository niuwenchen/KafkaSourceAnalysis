package com.jackniu.kafka.common.errors;

import java.util.HashSet;
import java.util.Set;

public class InvalidTopicException extends ApiException {
    private static final long serialVersionUID = 1L;

    private final Set<String> invalidTopics;

    public InvalidTopicException() {
        super();
        invalidTopics = new HashSet<>();
    }

    public InvalidTopicException(String message, Throwable cause) {
        super(message, cause);
        invalidTopics = new HashSet<>();
    }

    public InvalidTopicException(String message) {
        super(message);
        invalidTopics = new HashSet<>();
    }

    public InvalidTopicException(Throwable cause) {
        super(cause);
        invalidTopics = new HashSet<>();
    }

    public InvalidTopicException(Set<String> invalidTopics) {
        super("Invalid topics: " + invalidTopics);
        this.invalidTopics = invalidTopics;
    }

    public Set<String> invalidTopics() {
        return invalidTopics;
    }
}

