package com.bank.kafka.fraud.topology;

import com.bank.kafka.common.Topics;
import com.bank.kafka.common.event.FraudAlert;
import com.bank.kafka.common.event.Severity;
import com.bank.kafka.common.event.TransactionEvent;
import com.bank.kafka.common.serde.BankSerdes;
import com.bank.kafka.common.serde.TransactionTimestampExtractor;
import com.bank.kafka.fraud.config.FraudRulesProperties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Real-time fraud detection on posted transactions. Four independent rules, each showing a
 * different Kafka Streams technique, merged into one alert stream:
 *
 * <ol>
 *   <li><b>LARGE_AMOUNT</b> - stateless {@code filter} + {@code mapValues}.</li>
 *   <li><b>VELOCITY</b> - stateful windowed {@code count} on tumbling windows.</li>
 *   <li><b>IMPOSSIBLE_TRAVEL</b> - Processor API with a custom key-value state store.</li>
 *   <li><b>MONEY_MULE</b> - windowed KStream-KStream {@code join} (deposit followed by debit).</li>
 * </ol>
 *
 * All alerts go to {@code bank.fraud.alerts}; HIGH/CRITICAL ones also open a case in
 * {@code bank.fraud.cases}.
 */
@Component
public class FraudDetectionTopology {

    public static final String VELOCITY_STORE = "velocity-counts";
    public static final String LAST_LOCATION_STORE = "last-location";

    private final FraudRulesProperties rules;

    public FraudDetectionTopology(FraudRulesProperties rules) {
        this.rules = rules;
    }

    @Autowired
    public void buildPipeline(StreamsBuilder builder) {
        Serde<String> keySerde = BankSerdes.key();
        Serde<TransactionEvent> txnSerde = BankSerdes.transaction();

        KStream<String, TransactionEvent> posted = builder.stream(Topics.TRANSACTIONS_POSTED,
                Consumed.with(keySerde, txnSerde)
                        .withTimestampExtractor(new TransactionTimestampExtractor())
                        .withName("posted-transactions"));

        // ---- Rule 1: large single transaction (stateless) ------------------------------------
        KStream<String, FraudAlert> largeAmount = posted
                .filter((accountId, txn) -> txn.amount().compareTo(rules.largeAmountThreshold()) >= 0,
                        Named.as("large-amount-filter"))
                .mapValues((accountId, txn) -> FraudAlert.of(accountId, txn.transactionId(), "LARGE_AMOUNT",
                        Severity.HIGH, txn.type() + " of " + txn.amount() + " " + txn.currency()
                                + " exceeds " + rules.largeAmountThreshold(), txn.timestamp()),
                        Named.as("large-amount-alert"));

        // ---- Rule 2: velocity - too many debits in a tumbling window -------------------------
        // Caching is disabled so every count update is emitted; otherwise the record cache could
        // merge count N and N+1 and the "== threshold + 1" check would miss the crossing.
        KStream<String, FraudAlert> velocity = posted
                .filter((accountId, txn) -> !txn.type().isCredit(), Named.as("debits-only"))
                .groupByKey(Grouped.with("velocity-by-account", keySerde, txnSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(rules.velocityWindow()))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(VELOCITY_STORE)
                        .withKeySerde(keySerde).withCachingDisabled())
                .toStream(Named.as("velocity-updates"))
                .filter((window, count) -> count != null && count == rules.velocityMaxTransactions() + 1L)
                .map((window, count) -> KeyValue.pair(window.key(), FraudAlert.of(window.key(), null, "VELOCITY",
                        Severity.MEDIUM, count + " debits between " + window.window().startTime() + " and "
                                + window.window().endTime(), window.window().endTime())),
                        Named.as("velocity-alert"));

        // ---- Rule 3: impossible travel - Processor API + state store -------------------------
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(LAST_LOCATION_STORE), keySerde, txnSerde));
        KStream<String, FraudAlert> impossibleTravel = posted.processValues(
                () -> new ImpossibleTravelProcessor(LAST_LOCATION_STORE, rules.travelWindow()),
                Named.as("impossible-travel"), LAST_LOCATION_STORE);

        // ---- Rule 4: money mule - stream-stream windowed join --------------------------------
        // A large deposit followed (within muleWindow, never before) by a large debit on the
        // same account. Both sides are buffered in window stores until the window expires.
        KStream<String, TransactionEvent> largeDeposits = posted.filter((accountId, txn) ->
                txn.type().isCredit() && txn.amount().compareTo(rules.muleMinAmount()) >= 0, Named.as("mule-in"));
        KStream<String, TransactionEvent> largeDebits = posted.filter((accountId, txn) ->
                !txn.type().isCredit() && txn.amount().compareTo(rules.muleMinAmount()) >= 0, Named.as("mule-out"));
        KStream<String, FraudAlert> moneyMule = largeDeposits.join(largeDebits,
                (deposit, debit) -> FraudAlert.of(deposit.accountId(), debit.transactionId(), "MONEY_MULE",
                        Severity.HIGH, "Deposit of " + deposit.amount() + " followed by " + debit.type() + " of "
                                + debit.amount() + " within " + rules.muleWindow().toMinutes() + " min",
                        debit.timestamp()),
                JoinWindows.ofTimeDifferenceWithNoGrace(rules.muleWindow()).before(Duration.ZERO),
                StreamJoined.<String, TransactionEvent, TransactionEvent>with(keySerde, txnSerde, txnSerde)
                        .withName("money-mule-join").withStoreName("money-mule-join-store"));

        // ---- Merge and route -----------------------------------------------------------------
        KStream<String, FraudAlert> alerts = largeAmount
                .merge(velocity, Named.as("merge-velocity"))
                .merge(impossibleTravel, Named.as("merge-travel"))
                .merge(moneyMule, Named.as("merge-mule"));

        alerts.to(Topics.FRAUD_ALERTS, Produced.with(keySerde, BankSerdes.fraudAlert()));
        alerts.filter((accountId, alert) -> alert.severity().opensCase(), Named.as("case-worthy"))
                .to(Topics.FRAUD_CASES, Produced.with(keySerde, BankSerdes.fraudAlert()));
    }
}
