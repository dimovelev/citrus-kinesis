package com.prime157.citrus.aws.sqs;

import static com.prime157.citrus.aws.sqs.KinesisEndpoint.HEADER_ARRIVAL_TIMESTAMP;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.HEADER_PARTITION_KEY;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.HEADER_SEQUENCE_NUMBER;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.HEADER_SHARD_ID;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.consol.citrus.context.TestContext;
import com.consol.citrus.endpoint.EndpointConfiguration;
import com.consol.citrus.message.DefaultMessage;
import com.consol.citrus.message.Message;
import com.consol.citrus.messaging.Consumer;

import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

public class KinesisConsumer implements Consumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisConsumer.class);
    private final String streamName;
    private final KinesisClient kinesis;
    private final EndpointConfiguration endpointConfiguration;
    private final Map<String, String> shardIterators = new HashMap<>();
    private final Deque<DefaultMessage> messages = new ArrayDeque<>();

    public KinesisConsumer(final String streamName, final KinesisClient kinesis,
            final EndpointConfiguration endpointConfiguration) {
        this.streamName = streamName;
        this.kinesis = kinesis;
        this.endpointConfiguration = endpointConfiguration;
    }

    @Override
    public Message receive(final TestContext context) {
        return receive(context, endpointConfiguration.getTimeout());
    }

    @Override
    public Message receive(final TestContext context, final long timeout) {
        connectIfNecessary();
        final long started = System.currentTimeMillis();
        while (messages.isEmpty()) {
            pollMessages();
            final long duration = System.currentTimeMillis() - started;
            if (duration > endpointConfiguration.getTimeout()) {
                LOGGER.warn("Failed to receive message on time");
                break;
            }
        }
        if (messages.isEmpty()) {
            return null;
        } else {
            return messages.pop();
        }
    }

    private void pollMessages() {
        final Map<String, String> newShardIterators = new HashMap<>();
        shardIterators.forEach((shardId, shardIterator) -> {
            final GetRecordsRequest req = GetRecordsRequest.builder().shardIterator(shardIterator).build();
            final GetRecordsResponse res = kinesis.getRecords(req);
            res.records().forEach(record -> addRecord(shardId, record));
            newShardIterators.put(shardId, res.nextShardIterator());
        });
        shardIterators.putAll(newShardIterators);
    }

    private void addRecord(final String shardId, final Record record) {
        final String payload = record.data().asUtf8String();
        LOGGER.info("Received message: stream=[{}], shard=[{}], sequenceNumber=[{}], partitionKey=[{}], payload=[{}]",
                streamName, shardId, record.sequenceNumber(), record.partitionKey(), payload);
        final Map<String, Object> headers = new HashMap<>();
        headers.put(HEADER_PARTITION_KEY, record.partitionKey());
        headers.put(HEADER_SEQUENCE_NUMBER, record.sequenceNumber());
        headers.put(HEADER_ARRIVAL_TIMESTAMP, record.approximateArrivalTimestamp());
        headers.put(HEADER_SHARD_ID, shardId);
        messages.add(new DefaultMessage(payload, headers));
    }

    private void connectIfNecessary() {
        if (shardIterators.isEmpty()) {
            synchronized ( shardIterators ) {
                if (shardIterators.isEmpty()) {
                    connect();
                }
            }
        }
    }

    private void connect() {
        LOGGER.info("Creating initial shard iterators");
        final StreamDescription stream = getStreamDescription();
        stream.shards().forEach(shard -> {
            final GetShardIteratorRequest req = GetShardIteratorRequest.builder() //
                    .streamName(streamName) //
                    .shardId(shard.shardId()) //
                    .shardIteratorType(ShardIteratorType.LATEST) //
                    .build();
            final GetShardIteratorResponse res = kinesis.getShardIterator(req);
            shardIterators.put(shard.shardId(), res.shardIterator());
        });
    }

    private StreamDescription getStreamDescription() {
        final DescribeStreamRequest req = DescribeStreamRequest.builder().streamName(streamName).build();
        final StreamDescription result = kinesis.describeStream(req).streamDescription();
        if (result.streamStatus() != StreamStatus.ACTIVE) {
            throw new IllegalStateException("Kinesis result " + streamName + " is in status " + result.streamStatus());
        }
        return result;
    }

    @Override
    public String getName() {
        return "kinesis-consumer-" + streamName;
    }

    public void reset() {
        pollMessages();
        messages.clear();
    }

    public void init() {
        LOGGER.info("Initializing");
        connectIfNecessary();
    }
}
