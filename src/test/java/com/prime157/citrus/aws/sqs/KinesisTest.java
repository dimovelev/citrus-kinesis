package com.prime157.citrus.aws.sqs;

import static com.prime157.citrus.aws.sqs.KinesisEndpoint.CONTEXT_SEQUENCE_NUMBER;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.CONTEXT_SHARD_ID;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.HEADER_PARTITION_KEY;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.HEADER_SEQUENCE_NUMBER;
import static com.prime157.citrus.aws.sqs.KinesisEndpoint.HEADER_SHARD_ID;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.consol.citrus.annotations.CitrusTest;
import com.consol.citrus.dsl.testng.TestNGCitrusTestRunner;

@Test
public class KinesisTest extends TestNGCitrusTestRunner {
    @Autowired
    private KinesisEndpoint endpoint;

    @CitrusTest
    public void integrationTest() {
        final String key = randomAlphanumeric(10);
        final String payload = randomAlphanumeric(100);

        send(action -> action.endpoint(endpoint).header(HEADER_PARTITION_KEY, key).payload(payload));

        receive(action -> action.endpoint(endpoint).payload(payload) //
                .header(HEADER_PARTITION_KEY, key) //
                .header(HEADER_SEQUENCE_NUMBER, "${" + CONTEXT_SEQUENCE_NUMBER + "}") //
                .header(HEADER_SHARD_ID, "${" + CONTEXT_SHARD_ID + "}") //
        );
    }

    @CitrusTest
    public void multipleInSequence() {
        final String key = "key";
        final List<String> payloads = Arrays.asList("payload1", "payload2", "payload3", "payload4");

        payloads.forEach(payload -> send(action -> action.endpoint(endpoint) //
                .header(HEADER_PARTITION_KEY, key) //
                .payload(payload) //
        ));

        payloads.forEach(payload -> receive(action -> action.endpoint(endpoint) //
                .header(HEADER_PARTITION_KEY, key) //
                .payload(payload) //
        ));
    }
}
