package com.bank.kafka.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * The central event of the platform. It travels through the whole pipeline and its
 * {@link #status()} changes as it moves from topic to topic:
 * REQUESTED -> VALIDATED -> POSTED | REJECTED.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TransactionEvent(
        String transactionId,
        String accountId,
        TransactionType type,
        BigDecimal amount,
        String currency,
        String counterpartyAccountId,
        String channel,
        String country,
        String merchant,
        Instant timestamp,
        TransactionStatus status,
        String reason,
        BigDecimal balanceAfter) {

    public static TransactionEvent request(String accountId, TransactionType type, BigDecimal amount,
                                           String currency, String channel, String country, String merchant) {
        return new TransactionEvent(UUID.randomUUID().toString(), accountId, type, amount, currency, null,
                channel, country, merchant, Instant.now(), TransactionStatus.REQUESTED, null, null);
    }

    public TransactionEvent withTransactionId(String id) {
        return new TransactionEvent(id, accountId, type, amount, currency, counterpartyAccountId, channel,
                country, merchant, timestamp, status, reason, balanceAfter);
    }

    public TransactionEvent withCounterparty(String counterparty) {
        return new TransactionEvent(transactionId, accountId, type, amount, currency, counterparty, channel,
                country, merchant, timestamp, status, reason, balanceAfter);
    }

    public TransactionEvent withTimestamp(Instant ts) {
        return new TransactionEvent(transactionId, accountId, type, amount, currency, counterpartyAccountId,
                channel, country, merchant, ts, status, reason, balanceAfter);
    }

    public TransactionEvent validated() {
        return withStatus(TransactionStatus.VALIDATED, null, null);
    }

    public TransactionEvent rejected(String why) {
        return withStatus(TransactionStatus.REJECTED, why, balanceAfter);
    }

    public TransactionEvent posted(BigDecimal newBalance) {
        return withStatus(TransactionStatus.POSTED, null, newBalance);
    }

    /**
     * Builds the credit leg of a transfer: same money, keyed by the counterparty account,
     * pointing back at the debited account.
     */
    public TransactionEvent creditLeg() {
        return new TransactionEvent(transactionId + "-IN", counterpartyAccountId, TransactionType.TRANSFER_IN,
                amount, currency, accountId, channel, country, merchant, timestamp,
                TransactionStatus.VALIDATED, null, null);
    }

    /** Amount with sign: positive for credits, negative for debits. */
    public BigDecimal signedAmount() {
        return type.isCredit() ? amount : amount.negate();
    }

    private TransactionEvent withStatus(TransactionStatus newStatus, String newReason, BigDecimal newBalance) {
        return new TransactionEvent(transactionId, accountId, type, amount, currency, counterpartyAccountId,
                channel, country, merchant, timestamp, newStatus, newReason, newBalance);
    }
}
