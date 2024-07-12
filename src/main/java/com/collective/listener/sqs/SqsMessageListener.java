package com.collective.listener.sqs;

import java.util.Collections;
import java.util.List;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class SqsMessageListener {

	@Autowired
    private AmazonSQS sqsClient;
	
	@Autowired
    private RestTemplate restTemplate;
	
	@Autowired
	private SlackServiceImpl slackService;

    private String queueUrl;
    
    private ExecutorService executorService;
    
	@Value("${api.url}")
	private String apiUrl;
	
	@Autowired
	private AwsConfig manager;
	
    Logger LOGGER = LoggerFactory.getLogger(SqsMessageListener.class);

    @PostConstruct
    public void init() {
        // Initializing executor service with a dynamic thread pool
        executorService = Executors.newCachedThreadPool();
        startPolling();
    }

    public void startPolling() {
    	queueUrl = manager.getSecrets().get("queue.url").textValue();
        // Submit a task for polling messages to the executor service
        executorService.submit(() -> {
            while (true) {
            	try {
	            	LOGGER.info("Inside executor service");
	                ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
	                        .withMaxNumberOfMessages(10) // Fetching 10 messages from queue
	                        .withWaitTimeSeconds(5); 
	                List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
	                if(messages.size() == 0) {
	                	LOGGER.info("No messages found", messages.size());
	                	 // Sleep if no messages are found
	                    Thread.sleep(1000);
	                }
	                for (Message message : messages) {
	                    // Process each message in a separate thread
	                    executorService.submit(() -> processMessage(message));
	                }
                // Simulate processing time
//                Thread.sleep(1000);
            	} catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // Handle interruption (e.g., shutdown of the service)
                } catch (Exception e) {
                     // Handle other exceptions
         			LOGGER.info("Exception occured while fetching queue " + ExceptionUtils.getStackTrace(e));
                 }
            }
        });
    }

    private void processMessage(Message message) {
        try {
            LOGGER.info("Processing Message: " + message.getBody());
            ResponseEntity<String> response = callRestAPI(apiUrl, message.getBody());
            if (response.getStatusCode().is2xxSuccessful()) {
            	// Delete the message from the queue after processing
                sqsClient.deleteMessage(queueUrl, message.getReceiptHandle());
            }
        }  catch (final HttpServerErrorException exception) {
        	if (exception.getStatusCode().value() == 504 ) {
        		LOGGER.info("Data Engineering Machine is down" + ExceptionUtils.getStackTrace(exception));
        		slackService.slackNotification("Data Engineering Machine is unresponsive for the queue", null);
        	}
        	if(exception.getStatusCode().value() == 500) {
        		LOGGER.info("Internal Server error for payload {} with exception {}" , message, ExceptionUtils.getStackTrace(exception));
        		slackService.slackNotification("Internal Server error for payload | " +message +" | with exception " + ExceptionUtils.getStackTrace(exception), null);
        	}
        	if(exception.getStatusCode().value() == 400) {
        		LOGGER.info("Bad Request for payload {} with exception {}" , message, ExceptionUtils.getStackTrace(exception));
        		slackService.slackNotification("Bad Request for payload | " +message + " | with exception "+ ExceptionUtils.getStackTrace(exception), null);
        	}
        	if(exception.getStatusCode().value() == 403 || exception.getStatusCode().value() == 401) {
        		LOGGER.info("UnAuthorized for payload {} with exception {}" , message, ExceptionUtils.getStackTrace(exception));
        		slackService.slackNotification("UnAuthorized for payload | " +message + " | with exception "+ ExceptionUtils.getStackTrace(exception), null);
        	}
        } catch (Exception e) {
            // Handle other exceptions
			LOGGER.info("Exception occured while processing queue " + ExceptionUtils.getStackTrace(e));
        }
    }
    
    private ResponseEntity<String> callRestAPI(final String apiURI , final String body) {
		final HttpHeaders headers = new HttpHeaders();
		headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        HttpEntity<String> requestEntity = new HttpEntity<>(body, headers);
        ResponseEntity<String> response = restTemplate.postForEntity(apiURI, requestEntity, String.class);
		return response;
	}
}
