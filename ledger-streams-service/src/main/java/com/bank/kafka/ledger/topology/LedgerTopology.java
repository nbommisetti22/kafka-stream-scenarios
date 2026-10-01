package com.bank.kafka.ledger.topology;

import com.bank.kafka.common.Topics;
import com.bank.kafka.common.event.AccountActivitySummary;
import com.bank.kafka.common.event.AccountEvent;
import com.bank.kafka.common.event.CustomerProfile;
import com.bank.kafka.common.event.EnrichedTransaction;
import com.bank.kafka.common.event.TransactionEvent;
import com.bank.kafka.common.event.TransactionStatus;
import com.bank.kafka.common.event.TransactionType;
import com.bank.kafka.common.serde.BankSerdes;
import com.bank.kafka.common.serde.TransactionTimestampExtractor;
import com.bank.kafka.ledger.config.LedgerProperties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Ledger topology:
 *
 * <pre>
 *  bank.transactions.validated ──► [LedgerProcessor + "ledger-balances" store]
 *                                         │ split()
 *                    ┌────────────────────┴──────────────────┐
 *                 POSTED                                  REJECTED ──► bank.transactions.rejected
 *                    │
 *                    ├──► bank.transactions.posted
 *                    ├──► mapValues ──► bank.balances (compacted)
 *                    ├──► TRANSFER ? creditLeg re-keyed to counterparty ──► bank.transactions.validated
 *                    ├──► join KTable(accounts) ─► leftJoin GlobalKTable(customers) ──► bank.transactions.enriched
 *                    └──► groupByKey ─► 1h tumbling window aggregate ─► suppress ──► bank.account.activity-summary
 * </pre>
 */
@Component
public class LedgerTopology {

    public static final String LEDGER_STORE = "ledger-balances";
    public static final String ACCOUNTS_STORE = "accounts-table";
    public static final String CUSTOMERS_STORE = "customers-global";
    public static final String ACTIVITY_STORE = "account-activity";

    private final LedgerProperties props;

    public LedgerTopology(LedgerProperties props) {
        this.props = props;
    }

    @Autowired
    public void buildPipeline(StreamsBuilder builder) {
        Serde<String> keySerde = BankSerdes.key();
        Serde<TransactionEvent> txnSerde = BankSerdes.transaction();

        // ---- 1. Stateful ledger with the Processor API --------------------------------------
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(LEDGER_STORE), keySerde, BankSerdes.balance()));

        KStream<String, TransactionEvent> validated = builder.stream(Topics.TRANSACTIONS_VALIDATED,
                Consumed.with(keySerde, txnSerde)
                        .withTimestampExtractor(new TransactionTimestampExtractor())
                        .withName("validated-transactions"));

        KStream<String, TransactionEvent> booked = validated.processValues(
                () -> new LedgerProcessor(LEDGER_STORE, props.overdraftLimit()), Named.as("ledger"), LEDGER_STORE);

        // ---- 2. Branching: route by outcome -------------------------------------------------
        Map<String, KStream<String, TransactionEvent>> outcome = booked.split(Named.as("outcome-"))
                .branch((accountId, txn) -> txn.status() == TransactionStatus.POSTED, Branched.as("posted"))
                .defaultBranch(Branched.as("rejected"));
        KStream<String, TransactionEvent> posted = outcome.get("outcome-posted");
        KStream<String, TransactionEvent> rejected = outcome.get("outcome-rejected");

        posted.to(Topics.TRANSACTIONS_POSTED, Produced.with(keySerde, txnSerde));
        rejected.to(Topics.TRANSACTIONS_REJECTED, Produced.with(keySerde, txnSerde));

        // ---- 3. Balance changelog for other services (compacted topic) ----------------------
        // A second processor connected to the same store reads the snapshot just written.
        posted.processValues(() -> new BalanceLookupProcessor(LEDGER_STORE), Named.as("balance-lookup"), LEDGER_STORE)
                .to(Topics.BALANCES, Produced.with(keySerde, BankSerdes.balance()));

        // ---- 4. Transfers: re-key the credit leg to the counterparty account ----------------
        // Writing back to the input topic moves the record to the partition that owns the target
        // account. Within the EOS transaction, the debit and the credit request commit together.
        posted.filter((accountId, txn) -> txn.type() == TransactionType.TRANSFER, Named.as("transfers-only"))
                .map((accountId, txn) -> KeyValue.pair(txn.counterpartyAccountId(), txn.creditLeg()),
                        Named.as("transfer-credit-leg"))
                .to(Topics.TRANSACTIONS_VALIDATED, Produced.with(keySerde, txnSerde));

        // ---- 5. Enrichment: KStream-KTable join + KStream-GlobalKTable join -----------------
        // bank.accounts and bank.transactions.posted are co-partitioned (same key + partition count),
        // so a regular KTable join works. Customers are keyed by customerId (a different key), so a
        // GlobalKTable - fully replicated to every instance - is used instead.
        KTable<String, AccountEvent> accounts = builder.table(Topics.ACCOUNTS,
                Consumed.with(keySerde, BankSerdes.account()),
                Materialized.<String, AccountEvent, KeyValueStore<Bytes, byte[]>>as(ACCOUNTS_STORE)
                        .withKeySerde(keySerde).withValueSerde(BankSerdes.account()));
        GlobalKTable<String, CustomerProfile> customers = builder.globalTable(Topics.CUSTOMERS,
                Consumed.with(keySerde, BankSerdes.customer()),
                Materialized.<String, CustomerProfile, KeyValueStore<Bytes, byte[]>>as(CUSTOMERS_STORE)
                        .withKeySerde(keySerde).withValueSerde(BankSerdes.customer()));

        posted.join(accounts, EnrichedTransaction::of,
                        Joined.<String, TransactionEvent, AccountEvent>with(keySerde, txnSerde, BankSerdes.account())
                                .withName("join-account"))
                .leftJoin(customers,
                        (accountId, enriched) -> enriched.customerId(),   // foreign key into the GlobalKTable
                        (enriched, customer) -> enriched.withCustomer(customer),
                        Named.as("join-customer"))
                .to(Topics.TRANSACTIONS_ENRICHED, Produced.with(keySerde, BankSerdes.enriched()));

        // ---- 6. Windowed aggregation: hourly activity per account ---------------------------
        // Tumbling event-time windows; suppress() emits exactly one final result per window once
        // the window + grace period has passed, instead of one update per transaction.
        posted.groupByKey(Grouped.with("activity-by-account", keySerde, txnSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(props.summaryWindow(), props.summaryGrace()))
                .aggregate(AccountActivitySummary::empty,
                        (accountId, txn, summary) -> summary.add(accountId, txn),
                        Materialized.<String, AccountActivitySummary, WindowStore<Bytes, byte[]>>as(ACTIVITY_STORE)
                                .withKeySerde(keySerde).withValueSerde(BankSerdes.activitySummary()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()).withName("final-activity"))
                .toStream()
                .map((window, summary) -> KeyValue.pair(window.key(),
                        summary.withWindow(window.window().startTime(), window.window().endTime())))
                .to(Topics.ACCOUNT_ACTIVITY_SUMMARY, Produced.with(keySerde, BankSerdes.activitySummary()));
    }
}
