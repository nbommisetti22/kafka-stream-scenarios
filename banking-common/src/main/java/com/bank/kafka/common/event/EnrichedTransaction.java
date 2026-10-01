package com.bank.kafka.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** A posted transaction joined with account (KTable) and customer (GlobalKTable) data. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EnrichedTransaction(
        TransactionEvent transaction,
        String customerId,
        String accountType,
        String customerName,
        String customerEmail,
        String customerTier) {

    public static EnrichedTransaction of(TransactionEvent txn, AccountEvent account) {
        return new EnrichedTransaction(txn, account.customerId(), account.accountType(), null, null, null);
    }

    public EnrichedTransaction withCustomer(CustomerProfile customer) {
        if (customer == null) {
            return this;
        }
        return new EnrichedTransaction(transaction, customerId, accountType, customer.fullName(),
                customer.email(), customer.tier());
    }
}
