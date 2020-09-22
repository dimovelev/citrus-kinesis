package com.prime157.citrus.aws.sqs;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.KINESIS;
import static software.amazon.awssdk.core.SdkSystemSetting.CBOR_ENABLED;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

@Configuration
@ComponentScan("com.prime157.citrus.aws.sqs")
public class CitrusConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(CitrusConfiguration.class);

    @Bean
    public LocalStackContainer localStack() {
        final LocalStackContainer result = new LocalStackContainer();
        result //
                .withServices(KINESIS) //
                .withEnv("DEFAULT_REGION", "us-east-1") //
                .withEnv("HOSTNAME_EXTERNAL", "localhost") //
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("localstack")));

        result.start();
        return result;
    }

    @Bean
    public KinesisClient kinesisClient(final LocalStackContainer localStackContainer) {
        final URI uri = localStackContainer.getEndpointOverride(KINESIS);
        final AwsCredentials awsCredentials = AwsBasicCredentials.create("accesskey", "secretkey");
        final ProxyConfiguration proxy =
                ProxyConfiguration.builder().nonProxyHosts(new HashSet<>(Collections.singletonList("127.0.01")))
                        .build();
        final ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder() //
                .proxyConfiguration(proxy) //
                .connectionTimeout(Duration.ofSeconds(1)) //
                .socketTimeout(Duration.ofSeconds(10));
        System.setProperty(CBOR_ENABLED.property(), "false");
        return KinesisClient.builder() //
                .endpointOverride(uri) //
                .credentialsProvider(StaticCredentialsProvider.create(awsCredentials)) //
                .httpClientBuilder(httpClientBuilder) //
                .region(Region.US_EAST_1) //
                .build();
    }

    @Bean
    public KinesisEndpoint kinesisEndpoint(final KinesisClient kinesisClient) {
        final String streamName = randomAlphabetic(20);
        final CreateStreamRequest req = CreateStreamRequest.builder().streamName(streamName).shardCount(5).build();
        kinesisClient.createStream(req);
        waitForStreamToBecomeActive(kinesisClient, streamName);
        return new KinesisEndpoint(new KinesisEndpointConfiguration(Duration.ofSeconds(1), true), streamName,
                kinesisClient);
    }

    protected void waitForStreamToBecomeActive(final KinesisClient kinesis, final String streamName) {
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
}
