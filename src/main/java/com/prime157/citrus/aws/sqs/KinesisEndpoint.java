package com.prime157.citrus.aws.sqs;

import com.consol.citrus.endpoint.AbstractEndpoint;
import com.consol.citrus.messaging.Consumer;
import com.consol.citrus.messaging.Producer;

import software.amazon.awssdk.services.kinesis.KinesisClient;

public class KinesisEndpoint extends AbstractEndpoint {
    /**
     * The message header where the partition key is stored. If not provided with a produced record, a random one will
     * be generated.
     */
    public static final String HEADER_PARTITION_KEY = "kinesis.partitionKey";
    /**
     * The sequence number of the received message or the sequenceNumberForOrdering to use in the request.
     */
    public static final String HEADER_SEQUENCE_NUMBER = "kinesis.sequenceNumber";
    public static final String HEADER_HASH_KEY = "kinesis.hashKey";
    public static final String HEADER_ARRIVAL_TIMESTAMP = "kinesis.arrivalTimestamp";
    public static final String HEADER_SHARD_ID = "kinesis.shardId";
    public static final String CONTEXT_SHARD_ID = "kinesis.shardId";
    public static final String CONTEXT_SEQUENCE_NUMBER = "kinesis.sequenceNumber";

    private final String streamName;
    private final KinesisClient kinesis;
    private final KinesisConsumer kinesisConsumer;

    public KinesisEndpoint(final KinesisEndpointConfiguration endpointConfiguration, final String streamName,
            final KinesisClient kinesis) {
        super(endpointConfiguration);
        this.streamName = streamName;
        this.kinesis = kinesis;
        kinesisConsumer = new KinesisConsumer(streamName, kinesis, getEndpointConfiguration());
        if (endpointConfiguration.isAutostart()) {
            kinesisConsumer.init();
        }
    }

    @Override
    public Producer createProducer() {
        return new KinesisProducer(streamName, kinesis);
    }

    @Override
    public Consumer createConsumer() {
        return kinesisConsumer;
    }
}
