package com.bank.kafka.fraud.topology;

import com.bank.kafka.common.Topics;
import com.bank.kafka.common.event.FraudAlert;
import com.bank.kafka.common.event.Severity;
import com.bank.kafka.common.event.TransactionEvent;
import com.bank.kafka.common.event.TransactionStatus;
import com.bank.kafka.common.event.TransactionType;
import com.bank.kafka.common.serde.BankSerdes;
import com.bank.kafka.fraud.config.FraudRulesProperties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class FraudDetectionTopologyTest {

    private static final Instant T0 = Instant.parse("2026-01-01T10:00:00Z");
    private final AtomicInteger ids = new AtomicInteger();

    private TopologyTestDriver driver;
    private TestInputTopic<String, TransactionEvent> posted;
    private TestOutputTopic<String, FraudAlert> alerts;
    private TestOutputTopic<String, FraudAlert> cases;

    @BeforeEach
    void setUp() {
        FraudRulesProperties rules = new FraudRulesProperties(new BigDecimal("10000"), Duration.ofMinutes(1), 5,
                Duration.ofMinutes(30), Duration.ofMinutes(30), new BigDecimal("5000"));
        StreamsBuilder builder = new StreamsBuilder();
        new FraudDetectionTopology(rules).buildPipeline(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(builder.build(), props);

        posted = driver.createInputTopic(Topics.TRANSACTIONS_POSTED, new StringSerializer(),
                BankSerdes.transaction().serializer());
        alerts = driver.createOutputTopic(Topics.FRAUD_ALERTS, new StringDeserializer(),
                BankSerdes.fraudAlert().deserializer());
        cases = driver.createOutputTopic(Topics.FRAUD_CASES, new StringDeserializer(),
                BankSerdes.fraudAlert().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void normalActivityRaisesNoAlert() {
        send("ACC-1", TransactionType.DEPOSIT, "2000", "US", T0);
        send("ACC-1", TransactionType.PAYMENT, "40", "US", T0.plusSeconds(600));
        send("ACC-1", TransactionType.WITHDRAWAL, "100", "US", T0.plusSeconds(1200));

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void largeAmountRaisesHighAlertAndOpensCase() {
        send("ACC-1", TransactionType.DEPOSIT, "15000", "US", T0);

        FraudAlert alert = alerts.readValue();
        assertThat(alert.rule()).isEqualTo("LARGE_AMOUNT");
        assertThat(alert.severity()).isEqualTo(Severity.HIGH);
        assertThat(cases.readValue().alertId()).isEqualTo(alert.alertId());
    }

    @Test
    void velocityAlertFiresOnceWhenThresholdIsCrossed() {
        for (int i = 0; i < 8; i++) {
            send("ACC-1", TransactionType.PAYMENT, "10", "US", T0.plusSeconds(i * 5L));
        }

        List<FraudAlert> raised = alerts.readValuesToList();
        assertThat(raised).extracting(FraudAlert::rule).containsExactly("VELOCITY");
        assertThat(raised.get(0).severity()).isEqualTo(Severity.MEDIUM);
        assertThat(cases.isEmpty()).as("MEDIUM does not open a case").isTrue();
    }

    @Test
    void velocityCountsAreIsolatedPerAccountAndWindow() {
        // 5 debits in each of two consecutive windows and on another account: never more than 5 per window.
        for (int i = 0; i < 5; i++) {
            send("ACC-1", TransactionType.PAYMENT, "10", "US", T0.plusSeconds(i));
            send("ACC-1", TransactionType.PAYMENT, "10", "US", T0.plusSeconds(60 + i));
            send("ACC-2", TransactionType.PAYMENT, "10", "US", T0.plusSeconds(i));
        }
        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void impossibleTravelIsDetected() {
        send("ACC-1", TransactionType.PAYMENT, "20", "US", T0);
        send("ACC-1", TransactionType.PAYMENT, "30", "SG", T0.plus(Duration.ofMinutes(12)));

        FraudAlert alert = alerts.readValue();
        assertThat(alert.rule()).isEqualTo(ImpossibleTravelProcessor.RULE);
        assertThat(alert.severity()).isEqualTo(Severity.CRITICAL);
        assertThat(alert.description()).contains("US").contains("SG").contains("12 min");
    }

    @Test
    void incomingTransferFromAbroadIsNotTravel() {
        send("ACC-1", TransactionType.PAYMENT, "20", "US", T0);
        send("ACC-1", TransactionType.TRANSFER_IN, "30", "SG", T0.plus(Duration.ofMinutes(1)));
        send("ACC-1", TransactionType.PAYMENT, "20", "US", T0.plus(Duration.ofMinutes(2)));

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void travelAfterWindowIsFine() {
        send("ACC-1", TransactionType.PAYMENT, "20", "US", T0);
        send("ACC-1", TransactionType.PAYMENT, "30", "FR", T0.plus(Duration.ofHours(9)));

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void moneyMulePatternIsDetectedWithStreamStreamJoin() {
        send("ACC-9", TransactionType.DEPOSIT, "8000", "US", T0);
        send("ACC-9", TransactionType.TRANSFER, "7900", "US", T0.plus(Duration.ofMinutes(10)));

        List<FraudAlert> raised = alerts.readValuesToList();
        assertThat(raised).extracting(FraudAlert::rule).containsExactly("MONEY_MULE");
    }

    @Test
    void debitBeforeDepositIsNotAMule() {
        send("ACC-9", TransactionType.WITHDRAWAL, "6000", "US", T0);
        send("ACC-9", TransactionType.DEPOSIT, "8000", "US", T0.plus(Duration.ofMinutes(5)));

        assertThat(alerts.isEmpty()).isTrue();
    }

    private void send(String account, TransactionType type, String amount, String country, Instant ts) {
        TransactionEvent txn = new TransactionEvent("t" + ids.incrementAndGet(), account, type,
                new BigDecimal(amount), "USD", null, "CARD", country, null, ts, TransactionStatus.POSTED, null, null);
        posted.pipeInput(account, txn);
    }
}
