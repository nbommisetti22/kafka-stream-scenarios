package com.bank.kafka.processor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

@ConfigurationProperties(prefix = "banking.processor")
public record ProcessorProperties(
        @DefaultValue("3") int maxRetries,
        @DefaultValue("500ms") Duration initialBackoff,
        @DefaultValue(".DLT") String dltSuffix,
        @DefaultValue("10000") int dedupCacheSize,
        @DefaultValue({"USD", "EUR", "GBP", "INR"}) Set<String> supportedCurrencies,
        @DefaultValue("50000") BigDecimal defaultChannelLimit,
        Map<String, BigDecimal> channelLimits) {

    public BigDecimal limitFor(String channel) {
        if (channelLimits == null || channel == null) {
            return defaultChannelLimit;
        }
        return channelLimits.getOrDefault(channel.toUpperCase(), defaultChannelLimit);
    }
}
