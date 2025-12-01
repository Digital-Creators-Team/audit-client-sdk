package com.logservice.audit;

public enum Result {
    SUCCESS("success"),
    FAILED("failed"),
    PENDING("pending");

    private final String value;

    Result(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
