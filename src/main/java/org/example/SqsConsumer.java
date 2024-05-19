package org.example;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.List;

import static org.example.SqsSequencingExample.QUEUE_URL;

@Slf4j
public class SqsConsumer implements Runnable {

    @Override
    public void run() {
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.EU_NORTH_1)
                .build();

        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .messageSystemAttributeNamesWithStrings("MessageGroupId", "MessageDeduplicationId")
                .queueUrl(QUEUE_URL)
                .build();

        while (true) {
            // Receive messages from the SQS queue
            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();

            for (Message message : messages) {
                try {
                    // Process the message
                    processMessage(message);

                    // Delete the message from the SQS queue
                    sqsClient.deleteMessage(r -> r.queueUrl(QUEUE_URL).receiptHandle(message.receiptHandle()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (messages.isEmpty()) {
                log.info("No messages in the queue!");
                break;
            }
        }
        // Close the SQS client
        sqsClient.close();
    }

    
    
    private void processMessage(Message message) {
        // Your message processing logic goes here
//        log.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        log.info("Message received: {} - {}", message.body(), message.attributes());
//        log.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");

    }
}
