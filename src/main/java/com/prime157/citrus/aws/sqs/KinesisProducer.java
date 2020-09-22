package com.prime157.citrus.aws.sqs;

import static com.prime157.citrus.aws.sqs.KinesisEndpoint.CONTEXT_SEQUENCE_NUMBER;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.CONTEXT_SHARD_ID;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.HEADER_HASH_KEY;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.HEADER_PARTITION_KEY;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.HEADER_SEQUENCE_NUMBER;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.consol.citrus.context.TestContext;
import com.consol.citrus.message.Message;
import com.consol.citrus.messaging.Producer;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

public class KinesisProducer implements Producer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisProducer.class);
    private final String streamName;
    private final KinesisClient kinesis;

    public KinesisProducer(final String streamName, final KinesisClient kinesis) {
        this.streamName = streamName;
        this.kinesis = kinesis;
    }

    @Override
    public void send(final Message message, final TestContext context) {
        final PutRecordRequest.Builder requestBuilder = PutRecordRequest.builder() //
                .streamName(streamName);
        final Object partitionKeyHeader = message.getHeader(HEADER_PARTITION_KEY);
        if (partitionKeyHeader instanceof String) {
            requestBuilder.partitionKey((String) partitionKeyHeader);
        } else {
            LOGGER.warn("You have not provided a partition key as message header. Generating a random one.");
            requestBuilder.partitionKey(UUID.randomUUID().toString());
        }
        final Object sequenceNumberHeader = message.getHeader(HEADER_SEQUENCE_NUMBER);
        if (sequenceNumberHeader instanceof String) {
            requestBuilder.sequenceNumberForOrdering((String) sequenceNumberHeader);
        }
        final Object hashKeyHeader = message.getHeader(HEADER_HASH_KEY);
        if (hashKeyHeader instanceof String) {
            requestBuilder.explicitHashKey((String) hashKeyHeader);
        }
        final String payload = message.getPayload(String.class);
        requestBuilder.data(SdkBytes.fromUtf8String(payload));

        LOGGER.info(
                "Producing message to stream [{}] with partitionKey=[{}], sequenceNumberForOrdering=[{}], explicitHashKey=[{}] and payload [{}]",
                streamName, partitionKeyHeader, sequenceNumberHeader, hashKeyHeader, payload);
        final PutRecordRequest req = requestBuilder.build();
        final PutRecordResponse response = kinesis.putRecord(req);
        LOGGER.info("Successfully sent record to kinesis stream [{}] to shard [{}] with sequenceNumber [{}]",
                streamName, response.shardId(), response.sequenceNumber());
        context.setVariable(CONTEXT_SHARD_ID, response.shardId());
        context.setVariable(CONTEXT_SEQUENCE_NUMBER, response.sequenceNumber());
    }

    @Override
    public String getName() {
        return "kinesis-producer-" + streamName;
    }
}
