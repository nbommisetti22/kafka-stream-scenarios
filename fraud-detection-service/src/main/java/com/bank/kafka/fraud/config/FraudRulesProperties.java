package com.bank.kafka.fraud.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.math.BigDecimal;
import java.time.Duration;

@ConfigurationProperties(prefix = "banking.fraud")
public record FraudRulesProperties(
        /* LARGE_AMOUNT: any single transaction at or above this amount. */
        @DefaultValue("10000") BigDecimal largeAmountThreshold,
        /* VELOCITY: more than maxTransactions debits inside one window. */
        @DefaultValue("1m") Duration velocityWindow,
        @DefaultValue("5") int velocityMaxTransactions,
        /* IMPOSSIBLE_TRAVEL: card used in two countries within this time. */
        @DefaultValue("30m") Duration travelWindow,
        /* MONEY_MULE: large deposit followed by a large debit within this time. */
        @DefaultValue("30m") Duration muleWindow,
        @DefaultValue("5000") BigDecimal muleMinAmount) {
}
