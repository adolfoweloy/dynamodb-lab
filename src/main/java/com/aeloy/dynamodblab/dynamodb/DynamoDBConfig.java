package com.aeloy.dynamodblab.dynamodb;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DynamoDBConfig {

    /**
     * Allows the usage of DynamoDB Document API.
     *
     * @param amazonDynamoDB
     * @return
     */
    @Bean
    public DynamoDB dynamoDB(AmazonDynamoDB amazonDynamoDB) {
        return new DynamoDB(amazonDynamoDB);
    }

    @Bean
    public AmazonDynamoDB amazonDynamoDBClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withClientConfiguration(clientConfiguration())
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();
    }

    /**
     * Configures {@code ClientConfiguration} bean with full jitter backoff strategy to avoid
     * successive collisions.
     *
     * @return {@code ClientConfiguration} with backoff policy configured
     */
    @Bean
    public ClientConfiguration clientConfiguration() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        RetryPolicy.RetryCondition retryCondition = new PredefinedRetryPolicies.SDKDefaultRetryCondition();
        RetryPolicy.BackoffStrategy backoffStrategy = new PredefinedBackoffStrategies.FullJitterBackoffStrategy(2 * 1000, 30 * 1000);
        int maxErrorRetry = 50;

        RetryPolicy retryPolicy = new RetryPolicy(
                retryCondition,
                backoffStrategy,
                maxErrorRetry,
                true
        );

        clientConfiguration.setRetryPolicy(retryPolicy);
        return clientConfiguration;
    }

}
