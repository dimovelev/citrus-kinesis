package com.prime157.citrus.aws.sqs;

import static com.prime157.citrus.aws.sqs.KinesisEndpoint.CONTEXT_SEQUENCE_NUMBER;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.CONTEXT_SHARD_ID;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.HEADER_ARRIVAL_TIMESTAMP;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.HEADER_PARTITION_KEY;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.HEADER_SEQUENCE_NUMBER;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.HEADER_SHARD_ID;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.KINESIS;
import static software.amazon.awssdk.core.SdkSystemSetting.CBOR_ENABLED;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;

import org.junit.AfterClass;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import com.consol.citrus.context.TestContext;
import com.consol.citrus.endpoint.EndpointConfiguration;
import com.consol.citrus.message.Message;
import com.consol.citrus.message.RawMessage;
import com.consol.citrus.messaging.Consumer;
import com.consol.citrus.messaging.Producer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.CreateStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

class KinesisEndpointTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEndpointTest.class);
    private static LocalStackContainer localstack;
    private static KinesisClient kinesis;
    private String streamName;
    private TestContext context;

    private KinesisEndpoint testee;

    @BeforeAll
    static void beforeAll() {
        localstack = new LocalStackContainer() //
                .withServices(KINESIS) //
                .withEnv("DEFAULT_REGION", "us-east-1") //
                .withEnv("HOSTNAME_EXTERNAL", "localhost") //
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("localstack")));

        localstack.start();

        final URI uri = localstack.getEndpointOverride(KINESIS);
        final AwsCredentials awsCredentials = AwsBasicCredentials.create("accesskey", "secretkey");
        final ProxyConfiguration proxy =
                ProxyConfiguration.builder().nonProxyHosts(new HashSet<>(Collections.singletonList("127.0.01")))
                        .build();
        final ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder() //
                .proxyConfiguration(proxy) //
                .connectionTimeout(Duration.ofSeconds(1)) //
                .socketTimeout(Duration.ofSeconds(10));
        System.setProperty(CBOR_ENABLED.property(), "false");
        kinesis = KinesisClient.builder() //
                .endpointOverride(uri) //
                .credentialsProvider(StaticCredentialsProvider.create(awsCredentials)) //
                .httpClientBuilder(httpClientBuilder) //
                .region(Region.US_EAST_1) //
                .build();
    }

    @BeforeEach
    void beforeEach() {
        streamName = randomAlphabetic(20);
        final CreateStreamRequest req = CreateStreamRequest.builder().streamName(streamName).shardCount(2).build();
        final CreateStreamResponse response = kinesis.createStream(req);
        waitForStreamToBecomeActive(streamName);
        testee = new KinesisEndpoint(new EndpointConfiguration() {
            @Override
            public long getTimeout() {
                return 2_000;
            }

            @Override
            public void setTimeout(final long timeout) {

            }
        }, streamName, kinesis);
        context = new TestContext();
    }

    protected void waitForStreamToBecomeActive(final String streamName) {
        while (true) {
            final StreamDescription streamDescription =
                    kinesis.describeStream(DescribeStreamRequest.builder().streamName(streamName).build())
                            .streamDescription();
            final StreamStatus status = streamDescription.streamStatus();
            if (status == StreamStatus.ACTIVE) {
                LOGGER.info("Stream [{}] is now ready", streamName);
                break;
            }
            LOGGER.info("Waiting for stream [{}] to become active. Status is still {}", streamName, status);
            try {
                Thread.sleep(200L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @AfterClass
    public static void afterAll() {
        localstack.close();
    }

    @Test
    void produceAndConsume() {
        final Consumer consumer = testee.createConsumer();

        final Producer producer = testee.createProducer();
        final String payload1 = randomAlphanumeric(20);
        final RawMessage producedMessage1 = new RawMessage(payload1);
        final String key1 = randomAlphanumeric(10);
        producedMessage1.setHeader(HEADER_PARTITION_KEY, key1);

        producer.send(producedMessage1, context);

        assertThat(context.getVariables()) //
                .containsKey(CONTEXT_SHARD_ID) //
                .containsKey(CONTEXT_SEQUENCE_NUMBER);
        final String sequenceNumber1 = context.getVariable(CONTEXT_SEQUENCE_NUMBER);

        final Message received1 = consumer.receive(context);
        assertThat(received1.getPayload(String.class)).isEqualTo(payload1);
        assertThat(received1.getHeaders()) //
                .containsEntry(HEADER_PARTITION_KEY, key1) //
                .containsEntry(HEADER_SEQUENCE_NUMBER, sequenceNumber1) // the same as put in context by send
                .containsKey(HEADER_ARRIVAL_TIMESTAMP) //
                .containsKey(HEADER_SHARD_ID);

        final String payload2 = randomAlphanumeric(20);
        final RawMessage producedMessage2 = new RawMessage(payload2);
        final String key2 = randomAlphanumeric(10);
        producedMessage2.setHeader(HEADER_PARTITION_KEY, key2);

        producer.send(producedMessage2, context);

        final String sequenceNumber2 = context.getVariable(CONTEXT_SEQUENCE_NUMBER);

        final Message received2 = consumer.receive(context);
        assertThat(received2.getPayload(String.class)).isEqualTo(payload2);
        assertThat(received2.getHeaders()) //
                .containsEntry(HEADER_PARTITION_KEY, key2) //
                .containsEntry(HEADER_SEQUENCE_NUMBER, sequenceNumber2) // the same as put in context by send
                .containsKey(HEADER_ARRIVAL_TIMESTAMP) //
                .containsKey(HEADER_SHARD_ID);
    }

}
