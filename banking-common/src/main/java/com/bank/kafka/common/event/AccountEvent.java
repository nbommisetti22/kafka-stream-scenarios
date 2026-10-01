package com.bank.kafka.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

/** Latest state of a bank account. Published to a compacted topic keyed by accountId. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AccountEvent(
        String accountId,
        String customerId,
        String accountType,
        String currency,
        AccountStatus status,
        Instant updatedAt) {

    public AccountEvent withStatus(AccountStatus newStatus) {
        return new AccountEvent(accountId, customerId, accountType, currency, newStatus, Instant.now());
    }
}
