package org.example;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class SqsSequencingExample {

    private static final int NUM_THREADS = 5; // Number of threads for concurrent processing
    static final String QUEUE_URL = "local-test-queue.fifo";

    public static void main(String[] args) throws InterruptedException {

        // Set up the AWS SQS client
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.EU_NORTH_1)
                .build();


        log.info("XXXXXXXXXXXXXXXXXX -- sending message");
        // Send a message to the SQS queue
        for (int i = 0; i <= 20; i++) {
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(QUEUE_URL)
                    .messageBody("message " + i)
                    // with random UUID message sequence is random when consuming but when messageGroupId it sequence
                    //  is maintained within the group.
//                    .messageGroupId(java.util.UUID.randomUUID().toString())
                    .messageGroupId("group:" + i % 2)
                    .messageDeduplicationId(Instant.now() + "--" + i)
                    .build();
            sqsClient.sendMessage(sendMessageRequest);
        }

        Thread.sleep(500);

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            executor.execute(new SqsConsumer());
        }

        // Shutdown the executor after all tasks are completed
        executor.shutdown();
    }

}

