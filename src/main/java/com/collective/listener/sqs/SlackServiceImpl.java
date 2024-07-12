package com.collective.listener.sqs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.hubspot.slack.client.SlackClient;
import com.hubspot.slack.client.SlackClientFactory;
import com.hubspot.slack.client.SlackClientRuntimeConfig;
import com.hubspot.slack.client.methods.params.chat.ChatPostMessageParams;

@Service
public class SlackServiceImpl {

	@Autowired
	private AwsConfig manager;
	
    SlackClientRuntimeConfig runtimeConfig = SlackClientRuntimeConfig.builder()
            .setTokenSupplier(() -> manager.getSecrets().get("slack.client_id").textValue()).build();
    
    SlackClient slackClient = SlackClientFactory.defaultFactory().build(runtimeConfig);

    Logger LOGGER = LoggerFactory.getLogger(SlackServiceImpl.class);

    private final String channel = "queue-monitoring";

    public void slackNotification(final String message,  String channel) {
        if( channel == null ) channel =  this.channel;
        LOGGER.debug("Sending message to channel {}: {}", channel , message);
        slackClient.postMessage(ChatPostMessageParams.builder().setText(message).setChannelId(channel).build()).join()
                .unwrapOrElseThrow();
    }
}
