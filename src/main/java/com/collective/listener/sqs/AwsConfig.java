package com.collective.listener.sqs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.DecryptionFailureException;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.InternalServiceErrorException;
import com.amazonaws.services.secretsmanager.model.InvalidParameterException;
import com.amazonaws.services.secretsmanager.model.InvalidRequestException;
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class AwsConfig {

    private Logger LOGGER = LoggerFactory.getLogger(AwsConfig.class);

	private JsonNode secretsJson = null;
	
	@Value("${aws.manager.secret_name}")
	public String secretId;
	
	@Value("${aws.manager.region}")
	private String region;
	
	private String accessKey;
	private String secretKey;
	
    @Bean
    public AmazonSQS amazonSQS() {
        return AmazonSQSClientBuilder.standard()
                .withRegion(region).build();
    }
    
    @Bean
    @Primary
    public AWSCredentialsProvider buildAWSCredentialsProvider() {
    	// Building AWS Access for SQS 
    	accessKey = getSecrets().get("aws.credentials.accesskey").textValue();
		secretKey = getSecrets().get("aws.credentials.secretKey").textValue();
		AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
		return new AWSStaticCredentialsProvider(awsCredentials);
    }

    
	public JsonNode getSecrets() {
		if (secretsJson == null) {
			
			AWSSecretsManager client = AWSSecretsManagerClientBuilder.standard()
					.withRegion(region).build();
			String secret = null;
			GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretId);
			GetSecretValueResult getSecretValueResult = null;
			ObjectMapper objectMapper = new ObjectMapper();

			try {
				getSecretValueResult = client.getSecretValue(getSecretValueRequest);
			} catch (DecryptionFailureException e) {
				throw e;
			} catch (InternalServiceErrorException e) {
				throw e;
			} catch (InvalidParameterException e) {
				throw e;
			} catch (InvalidRequestException e) {
				throw e;
			} catch (ResourceNotFoundException e) {
				throw e;
			}

			if (getSecretValueResult.getSecretString() != null) {
				secret = getSecretValueResult.getSecretString();
				if (secret != null) {
					try {
						secretsJson = objectMapper.readTree(secret);
					} catch (IOException e) {
						LOGGER.error("Exception while retrieving secret values: " + e.getMessage());
					}
				}
			}
		}
		return secretsJson;
	}
}
