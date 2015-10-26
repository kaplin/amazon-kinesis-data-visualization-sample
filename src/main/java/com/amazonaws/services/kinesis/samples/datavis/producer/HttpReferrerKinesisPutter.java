/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.samples.datavis.producer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.kinesis.samples.datavis.model.HealthCheckStateMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Sends HTTP referrer pairs to Amazon Kinesis.
 */
public class HttpReferrerKinesisPutter {
    private static final Log LOG = LogFactory.getLog(HttpReferrerKinesisPutter.class);

    private MessageFactory messageFactory;
    private AmazonKinesis kinesis;
    private String streamName;

    private final ObjectMapper JSON = new ObjectMapper();

    private ExecutorService executorService = Executors.newFixedThreadPool(10);
    private AtomicInteger sent = new AtomicInteger();
    private AtomicInteger submitted = new AtomicInteger();
    private Random random = new Random();

    public HttpReferrerKinesisPutter(MessageFactory messageFactory, AmazonKinesis kinesis, String streamName) {
        if (messageFactory == null) {
            throw new IllegalArgumentException("pairFactory must not be null");
        }
        if (kinesis == null) {
            throw new IllegalArgumentException("kinesis must not be null");
        }
        if (streamName == null || streamName.isEmpty()) {
            throw new IllegalArgumentException("streamName must not be null or empty");
        }
        this.messageFactory = messageFactory;
        this.kinesis = kinesis;
        this.streamName = streamName;
    }

    /**
     * Send a fixed number of HTTP Referrer pairs to Amazon Kinesis. This sends them sequentially.
     * If you require more throughput consider using multiple {@link HttpReferrerKinesisPutter}s.
     *
     * @param n The number of pairs to send to Amazon Kinesis.
     * @param delayBetweenRecords The amount of time to wait in between sending records. If this is <= 0 it will be
     *        ignored.
     * @param unitForDelay The unit of time to interpret the provided delay as.
     *
     * @throws InterruptedException Interrupted while waiting to send the next pair.
     */
    public void sendPairs(long n, long delayBetweenRecords, TimeUnit unitForDelay) throws InterruptedException {
        for (int i = 0; i < n && !Thread.currentThread().isInterrupted(); i++) {
            sendMessage();
            Thread.sleep(unitForDelay.toMillis(delayBetweenRecords));
        }
    }

    /**
     * Continuously sends HTTP Referrer pairs to Amazon Kinesis sequentially. This will only stop if interrupted. If you
     * require more throughput consider using multiple {@link HttpReferrerKinesisPutter}s.
     *
     * @param delayBetweenRecords The amount of time to wait in between sending records. If this is <= 0 it will be
     *        ignored.
     * @param unitForDelay The unit of time to interpret the provided delay as.
     *
     * @param messagesToSendPerIteration
     * @throws InterruptedException Interrupted while waiting to send the next pair.
     */
    public void sendMessagesIndefinitely(long delayBetweenRecords, TimeUnit unitForDelay, int messagesToSendPerIteration) throws InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    sendMessage();
                    sent.incrementAndGet();
                }
            };
            for (int i = 0; i < messagesToSendPerIteration; i++) {
                executorService.submit(task);
                submitted.incrementAndGet();
            }
            if (delayBetweenRecords > 0) {
                Thread.sleep(unitForDelay.toMillis(delayBetweenRecords));
            }

            LOG.info("Messages submitted: " + submitted.get() + ", sent " + sent.get());
            sent.set(0);
            submitted.set(0);
        }
    }

    /**
     * Send a single pair to Amazon Kinesis using PutRecord.
     */
    private void sendMessage() {
        HealthCheckStateMessage message = messageFactory.create();
        byte[] bytes = new byte[5440 * 17];
//        try {
//            bytes = JSON.writeValueAsBytes(message);
//        } catch (IOException e) {
//            LOG.warn("Skipping pair. Unable to serialize: '" + message + "'", e);
//            return;
//        }

        PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(streamName);
        // We use the resource as the partition key so we can accurately calculate totals for a given resource
        putRecord.setPartitionKey(Integer.toString(message.getStartingSequenceNumber()));
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) random.nextInt(255);
        }
        putRecord.setData(ByteBuffer.wrap(bytes));
        // Order is not important for this application so we do not send a SequenceNumberForOrdering
        putRecord.setSequenceNumberForOrdering(null);

        try {
            kinesis.putRecord(putRecord);
        } catch (ProvisionedThroughputExceededException ex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Thread %s's Throughput exceeded. Waiting 10ms", Thread.currentThread().getName()));
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } catch (AmazonClientException ex) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
        }
    }
}
