package com.bank.kafka.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;

/** Current balance of an account as maintained by the ledger state store. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BalanceSnapshot(
        String accountId,
        BigDecimal balance,
        String currency,
        long transactionCount,
        String lastTransactionId,
        Instant updatedAt) {

    public static BalanceSnapshot opening(String accountId, String currency) {
        return new BalanceSnapshot(accountId, BigDecimal.ZERO, currency, 0, null, null);
    }

    public BalanceSnapshot apply(TransactionEvent txn, BigDecimal newBalance) {
        return new BalanceSnapshot(accountId, newBalance, currency != null ? currency : txn.currency(),
                transactionCount + 1, txn.transactionId(), txn.timestamp());
    }
}
