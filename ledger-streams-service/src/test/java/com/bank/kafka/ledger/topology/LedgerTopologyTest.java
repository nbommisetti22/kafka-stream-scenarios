package com.bank.kafka.ledger.topology;

import com.bank.kafka.common.Topics;
import com.bank.kafka.common.event.AccountActivitySummary;
import com.bank.kafka.common.event.AccountEvent;
import com.bank.kafka.common.event.AccountStatus;
import com.bank.kafka.common.event.BalanceSnapshot;
import com.bank.kafka.common.event.CustomerProfile;
import com.bank.kafka.common.event.EnrichedTransaction;
import com.bank.kafka.common.event.TransactionEvent;
import com.bank.kafka.common.event.TransactionStatus;
import com.bank.kafka.common.event.TransactionType;
import com.bank.kafka.common.serde.BankSerdes;
import com.bank.kafka.ledger.config.LedgerProperties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class LedgerTopologyTest {

    private static final Instant T0 = Instant.parse("2026-01-01T10:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, TransactionEvent> validated;
    private TestInputTopic<String, AccountEvent> accounts;
    private TestInputTopic<String, CustomerProfile> customers;
    private TestOutputTopic<String, TransactionEvent> posted;
    private TestOutputTopic<String, TransactionEvent> rejected;
    private TestOutputTopic<String, BalanceSnapshot> balances;
    private TestOutputTopic<String, EnrichedTransaction> enriched;
    private TestOutputTopic<String, AccountActivitySummary> summaries;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new LedgerTopology(new LedgerProperties(BigDecimal.ZERO, Duration.ofHours(1), Duration.ofMinutes(5)))
                .buildPipeline(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ledger-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        driver = new TopologyTestDriver(builder.build(), props);

        StringSerializer ks = new StringSerializer();
        StringDeserializer kd = new StringDeserializer();
        validated = driver.createInputTopic(Topics.TRANSACTIONS_VALIDATED, ks, BankSerdes.transaction().serializer());
        accounts = driver.createInputTopic(Topics.ACCOUNTS, ks, BankSerdes.account().serializer());
        customers = driver.createInputTopic(Topics.CUSTOMERS, ks, BankSerdes.customer().serializer());
        posted = driver.createOutputTopic(Topics.TRANSACTIONS_POSTED, kd, BankSerdes.transaction().deserializer());
        rejected = driver.createOutputTopic(Topics.TRANSACTIONS_REJECTED, kd, BankSerdes.transaction().deserializer());
        balances = driver.createOutputTopic(Topics.BALANCES, kd, BankSerdes.balance().deserializer());
        enriched = driver.createOutputTopic(Topics.TRANSACTIONS_ENRICHED, kd, BankSerdes.enriched().deserializer());
        summaries = driver.createOutputTopic(Topics.ACCOUNT_ACTIVITY_SUMMARY, kd,
                BankSerdes.activitySummary().deserializer());

        customers.pipeInput("CUST-1", new CustomerProfile("CUST-1", "Ada Lovelace", "ada@example.com", null, "GB", "GOLD"));
        accounts.pipeInput("ACC-1", account("ACC-1", "CUST-1"));
        accounts.pipeInput("ACC-2", account("ACC-2", "CUST-1"));
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void depositsAndWithdrawalsUpdateBalance() {
        send(txn("t1", "ACC-1", TransactionType.DEPOSIT, "1000.00", T0));
        send(txn("t2", "ACC-1", TransactionType.WITHDRAWAL, "250.50", T0.plusSeconds(10)));

        List<TransactionEvent> out = posted.readValuesToList();
        assertThat(out).extracting(TransactionEvent::status).containsOnly(TransactionStatus.POSTED);
        assertThat(out).extracting(TransactionEvent::balanceAfter)
                .containsExactly(new BigDecimal("1000.00"), new BigDecimal("749.50"));

        KeyValueStore<String, BalanceSnapshot> store = driver.getKeyValueStore(LedgerTopology.LEDGER_STORE);
        assertThat(store.get("ACC-1").balance()).isEqualByComparingTo("749.50");
        assertThat(store.get("ACC-1").transactionCount()).isEqualTo(2);

        List<BalanceSnapshot> snapshots = balances.readValuesToList();
        assertThat(snapshots).hasSize(2);
        assertThat(snapshots.get(1).balance()).isEqualByComparingTo("749.50");
    }

    @Test
    void overdraftIsRejected() {
        send(txn("t1", "ACC-1", TransactionType.DEPOSIT, "100", T0));
        send(txn("t2", "ACC-1", TransactionType.PAYMENT, "100.01", T0.plusSeconds(1)));

        assertThat(posted.readValuesToList()).hasSize(1);
        TransactionEvent r = rejected.readValue();
        assertThat(r.transactionId()).isEqualTo("t2");
        assertThat(r.reason()).startsWith("INSUFFICIENT_FUNDS");
        assertThat(driver.<String, BalanceSnapshot>getKeyValueStore(LedgerTopology.LEDGER_STORE).get("ACC-1").balance())
                .isEqualByComparingTo("100");
    }

    @Test
    void duplicateDeliveryIsAppliedOnce() {
        TransactionEvent deposit = txn("t1", "ACC-1", TransactionType.DEPOSIT, "100", T0);
        send(deposit);
        send(deposit);

        assertThat(posted.readValuesToList()).hasSize(1);
    }

    @Test
    void transferDebitsSourceAndCreditsCounterparty() {
        send(txn("t1", "ACC-1", TransactionType.DEPOSIT, "500", T0));
        send(txn("t2", "ACC-1", TransactionType.TRANSFER, "200", T0.plusSeconds(5)).withCounterparty("ACC-2"));

        List<TransactionEvent> out = posted.readValuesToList();
        assertThat(out).extracting(TransactionEvent::transactionId).containsExactly("t1", "t2", "t2-IN");
        TransactionEvent credit = out.get(2);
        assertThat(credit.accountId()).isEqualTo("ACC-2");
        assertThat(credit.type()).isEqualTo(TransactionType.TRANSFER_IN);
        assertThat(credit.counterpartyAccountId()).isEqualTo("ACC-1");

        KeyValueStore<String, BalanceSnapshot> store = driver.getKeyValueStore(LedgerTopology.LEDGER_STORE);
        assertThat(store.get("ACC-1").balance()).isEqualByComparingTo("300");
        assertThat(store.get("ACC-2").balance()).isEqualByComparingTo("200");
    }

    @Test
    void postedTransactionsAreEnrichedWithAccountAndCustomer() {
        send(txn("t1", "ACC-1", TransactionType.DEPOSIT, "42", T0));

        EnrichedTransaction e = enriched.readValue();
        assertThat(e.transaction().transactionId()).isEqualTo("t1");
        assertThat(e.customerId()).isEqualTo("CUST-1");
        assertThat(e.accountType()).isEqualTo("CHECKING");
        assertThat(e.customerName()).isEqualTo("Ada Lovelace");
        assertThat(e.customerTier()).isEqualTo("GOLD");
    }

    @Test
    void hourlySummaryIsEmittedOnceWhenWindowCloses() {
        send(txn("t1", "ACC-1", TransactionType.DEPOSIT, "1000", T0.plusSeconds(60)));
        send(txn("t2", "ACC-1", TransactionType.WITHDRAWAL, "100", T0.plusSeconds(120)));
        send(txn("t3", "ACC-1", TransactionType.PAYMENT, "50", T0.plusSeconds(180)));
        assertThat(summaries.isEmpty()).as("suppressed until window closes").isTrue();

        // Advance stream time past window end (11:00) + grace (5m).
        send(txn("t4", "ACC-1", TransactionType.DEPOSIT, "1", T0.plus(Duration.ofMinutes(66))));

        AccountActivitySummary s = summaries.readValue();
        assertThat(s.accountId()).isEqualTo("ACC-1");
        assertThat(s.windowStart()).isEqualTo(T0);
        assertThat(s.transactionCount()).isEqualTo(3);
        assertThat(s.totalCredits()).isEqualByComparingTo("1000");
        assertThat(s.totalDebits()).isEqualByComparingTo("150");
        assertThat(summaries.isEmpty()).isTrue();
    }

    private void send(TransactionEvent txn) {
        validated.pipeInput(txn.accountId(), txn);
    }

    private static AccountEvent account(String id, String customerId) {
        return new AccountEvent(id, customerId, "CHECKING", "USD", AccountStatus.ACTIVE, T0);
    }

    private static TransactionEvent txn(String id, String account, TransactionType type, String amount, Instant ts) {
        return new TransactionEvent(id, account, type, new BigDecimal(amount), "USD", null, "ONLINE", "US", null,
                ts, TransactionStatus.VALIDATED, null, null);
    }
}
