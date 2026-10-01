package com.bank.kafka.account;

import com.bank.kafka.account.api.Requests.TransactionRequest;
import com.bank.kafka.account.api.Requests.TransferRequest;
import com.bank.kafka.account.api.Responses.PublishedEvent;
import com.bank.kafka.common.BankHeaders;
import com.bank.kafka.common.Topics;
import com.bank.kafka.common.event.TransactionEvent;
import com.bank.kafka.common.event.TransactionType;
import com.bank.kafka.common.serde.JsonSerde;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, brokerProperties = {
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1"})
class AccountServiceIntegrationTest {

    private static final JsonSerde<TransactionEvent> TXN = new JsonSerde<>(TransactionEvent.class);

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private TestRestTemplate rest;

    private Consumer<String, byte[]> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> props = KafkaTestUtils.consumerProps("acct-it-" + System.nanoTime(), "false", broker);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new ByteArrayDeserializer())
                .createConsumer();
        consumer.subscribe(List.of(Topics.TRANSACTIONS_REQUESTED));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void singleTransactionIsKeyedByAccountAndCarriesHeaders() {
        TransactionRequest req = new TransactionRequest("idem-1", "ACC-1", TransactionType.DEPOSIT,
                new BigDecimal("150.00"), "USD", "MOBILE", "US", null);
        ResponseEntity<PublishedEvent> resp = rest.postForEntity("/api/transactions", req, PublishedEvent.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(resp.getBody().id()).isEqualTo("idem-1");

        ConsumerRecord<String, byte[]> record = poll(1, "ACC-1").get(0);
        assertThat(record.key()).isEqualTo("ACC-1");
        assertThat(record.partition()).isEqualTo(resp.getBody().partition());
        assertThat(header(record, BankHeaders.SOURCE_SYSTEM)).isEqualTo("account-service");
        assertThat(header(record, BankHeaders.CORRELATION_ID)).isNotBlank();
        assertThat(record.headers().lastHeader("__TypeId__")).as("no Java type leaks onto the wire").isNull();

        TransactionEvent txn = TXN.deserializer().deserialize("t", record.value());
        assertThat(txn.amount()).isEqualByComparingTo("150.00");
        assertThat(txn.channel()).isEqualTo("MOBILE");
    }

    @Test
    void sameAccountAlwaysLandsOnSamePartition() {
        TransactionRequest req = new TransactionRequest(null, "ACC-ORDER", TransactionType.PAYMENT,
                new BigDecimal("1.00"), "USD", null, null, null);
        int first = rest.postForEntity("/api/transactions", req, PublishedEvent.class).getBody().partition();
        for (int i = 0; i < 5; i++) {
            assertThat(rest.postForEntity("/api/transactions", req, PublishedEvent.class).getBody().partition())
                    .isEqualTo(first);
        }
    }

    @Test
    void transferIsPublishedOnSourceAccount() {
        TransferRequest req = new TransferRequest(null, "ACC-A", "ACC-B", new BigDecimal("75"), "USD", null, "US");
        rest.postForEntity("/api/transfers", req, PublishedEvent.class);

        ConsumerRecord<String, byte[]> record = poll(1, "ACC-A").get(0);
        TransactionEvent txn = TXN.deserializer().deserialize("t", record.value());
        assertThat(txn.type()).isEqualTo(TransactionType.TRANSFER);
        assertThat(txn.counterpartyAccountId()).isEqualTo("ACC-B");
    }

    @Test
    void batchIsCommittedAtomicallyWithSharedCorrelationId() {
        List<TransactionRequest> batch = List.of(
                new TransactionRequest(null, "ACC-P1", TransactionType.DEPOSIT, new BigDecimal("3000"), "USD", "BRANCH", "US", "Payroll"),
                new TransactionRequest(null, "ACC-P2", TransactionType.DEPOSIT, new BigDecimal("3100"), "USD", "BRANCH", "US", "Payroll"),
                new TransactionRequest(null, "ACC-P3", TransactionType.DEPOSIT, new BigDecimal("3200"), "USD", "BRANCH", "US", "Payroll"));
        ResponseEntity<List<PublishedEvent>> resp = rest.exchange("/api/transactions/batch", HttpMethod.POST,
                new HttpEntity<>(batch), new ParameterizedTypeReference<>() {
                });
        assertThat(resp.getBody()).hasSize(3);

        List<ConsumerRecord<String, byte[]>> records = poll(3, "ACC-P1", "ACC-P2", "ACC-P3");
        assertThat(records).extracting(ConsumerRecord::key).containsExactlyInAnyOrder("ACC-P1", "ACC-P2", "ACC-P3");
        assertThat(records).extracting(r -> header(r, BankHeaders.CORRELATION_ID)).containsOnly(
                header(records.get(0), BankHeaders.CORRELATION_ID));
    }

    /** Tests share the topic, so only collect records for the keys this test produced. */
    private List<ConsumerRecord<String, byte[]>> poll(int expected, String... keys) {
        List<String> wanted = List.of(keys);
        List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 15_000;
        while (out.size() < expected && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200)).forEach(r -> {
                if (wanted.contains(r.key())) {
                    out.add(r);
                }
            });
        }
        assertThat(out).hasSizeGreaterThanOrEqualTo(expected);
        return out;
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
