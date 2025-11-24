package com.logservice.audit;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Represents the result status of an audit event
 */
public enum Result {
    SUCCESS("success"),
    FAILED("failed"),
    PENDING("pending");

    private final String value;

    Result(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
