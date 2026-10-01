package com.bank.kafka.ledger.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.math.BigDecimal;
import java.time.Duration;

@ConfigurationProperties(prefix = "banking.ledger")
public record LedgerProperties(
        /* How far an account may go below zero. */
        @DefaultValue("0") BigDecimal overdraftLimit,
        /* Size of the tumbling window used for account activity summaries. */
        @DefaultValue("1h") Duration summaryWindow,
        /* How long late (out-of-order) events are still accepted into a closed window. */
        @DefaultValue("5m") Duration summaryGrace) {
}
