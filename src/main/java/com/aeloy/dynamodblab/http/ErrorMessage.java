package com.aeloy.dynamodblab.http;

/**
 * Represents an error message.
 */
public class ErrorMessage {
    private final String message;

    public ErrorMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
