package com.bank.kafka.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;

/** Aggregate of all posted transactions of one account inside one time window. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AccountActivitySummary(
        String accountId,
        Instant windowStart,
        Instant windowEnd,
        long transactionCount,
        BigDecimal totalCredits,
        BigDecimal totalDebits) {

    public static AccountActivitySummary empty() {
        return new AccountActivitySummary(null, null, null, 0, BigDecimal.ZERO, BigDecimal.ZERO);
    }

    public AccountActivitySummary add(String account, TransactionEvent txn) {
        BigDecimal credits = txn.type().isCredit() ? totalCredits.add(txn.amount()) : totalCredits;
        BigDecimal debits = txn.type().isCredit() ? totalDebits : totalDebits.add(txn.amount());
        return new AccountActivitySummary(account, windowStart, windowEnd, transactionCount + 1, credits, debits);
    }

    public AccountActivitySummary withWindow(Instant start, Instant end) {
        return new AccountActivitySummary(accountId, start, end, transactionCount, totalCredits, totalDebits);
    }

    public BigDecimal netFlow() {
        return totalCredits.subtract(totalDebits);
    }
}
