package com.aeloy.dynamodblab.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;

public class DynamoDBException extends RuntimeException {
    public DynamoDBException(AmazonDynamoDBException amazonException) {
        super(amazonException);
    }

    public DynamoDBException(String message) {
        super(message);
    }
}
