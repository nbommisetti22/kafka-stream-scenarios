package com.bank.kafka.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record FraudAlert(
        String alertId,
        String accountId,
        String transactionId,
        String rule,
        Severity severity,
        String description,
        Instant detectedAt) {

    public static FraudAlert of(String accountId, String transactionId, String rule, Severity severity,
                                String description, Instant detectedAt) {
        return new FraudAlert(UUID.randomUUID().toString(), accountId, transactionId, rule, severity,
                description, detectedAt);
    }
}
