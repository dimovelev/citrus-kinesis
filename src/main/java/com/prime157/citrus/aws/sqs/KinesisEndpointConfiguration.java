package com.prime157.citrus.aws.sqs;

import java.time.Duration;

import com.consol.citrus.endpoint.EndpointConfiguration;

public class KinesisEndpointConfiguration implements EndpointConfiguration {
    private Duration timeout;
    private boolean autostart = true;

    /**
     * @param timeout   the timeout for receive calls
     * @param autostart whether to start the consumer right away
     */
    public KinesisEndpointConfiguration(final Duration timeout, final boolean autostart) {
        this.timeout = timeout;
        this.autostart = autostart;
    }

    @Override
    public long getTimeout() {
        return timeout.toMillis();
    }

    @Override
    public void setTimeout(final long timeout) {
        this.timeout = Duration.ofMillis(timeout);
    }

    public boolean isAutostart() {
        return autostart;
    }

    public void setAutostart(final boolean autostart) {
        this.autostart = autostart;
    }
}
