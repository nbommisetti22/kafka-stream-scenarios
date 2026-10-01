package com.bank.kafka.common;

/**
 * Central catalogue of every topic in the banking platform.
 *
 * <p>All transactional topics are keyed by {@code accountId}. Topics that are joined
 * together in Kafka Streams ({@link #ACCOUNTS} and {@link #TRANSACTIONS_VALIDATED}) must be
 * <b>co-partitioned</b>: same key, same partition count, same partitioner.
 */
public final class Topics {

    /** Compacted. key = customerId, value = CustomerProfile (tombstone = GDPR delete). */
    public static final String CUSTOMERS = "bank.customers";

    /** Compacted. key = accountId, value = AccountEvent (latest state of the account). */
    public static final String ACCOUNTS = "bank.accounts";

    /** Raw transaction requests from channels (mobile, ATM, branch...). key = accountId. */
    public static final String TRANSACTIONS_REQUESTED = "bank.transactions.requested";

    /** Dead letter topic for requests that could not be processed (poison pills, exhausted retries). */
    public static final String TRANSACTIONS_REQUESTED_DLT = TRANSACTIONS_REQUESTED + ".DLT";

    /** Requests that passed business validation and are ready for the ledger. key = accountId. */
    public static final String TRANSACTIONS_VALIDATED = "bank.transactions.validated";

    /** Transactions booked on the ledger (balance updated). key = accountId. */
    public static final String TRANSACTIONS_POSTED = "bank.transactions.posted";

    /** Transactions rejected by validation or by the ledger (e.g. insufficient funds). key = accountId. */
    public static final String TRANSACTIONS_REJECTED = "bank.transactions.rejected";

    /** Posted transactions enriched with account and customer data. key = accountId. */
    public static final String TRANSACTIONS_ENRICHED = "bank.transactions.enriched";

    /** Compacted. key = accountId, value = BalanceSnapshot (changelog of balances). */
    public static final String BALANCES = "bank.balances";

    /** Windowed (hourly) activity summaries per account. key = accountId. */
    public static final String ACCOUNT_ACTIVITY_SUMMARY = "bank.account.activity-summary";

    /** All fraud alerts. key = accountId. */
    public static final String FRAUD_ALERTS = "bank.fraud.alerts";

    /** High / critical fraud alerts that open an investigation case. key = accountId. */
    public static final String FRAUD_CASES = "bank.fraud.cases";

    private Topics() {
    }
}
