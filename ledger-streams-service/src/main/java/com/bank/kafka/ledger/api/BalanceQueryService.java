package com.bank.kafka.ledger.api;

import com.bank.kafka.common.event.AccountActivitySummary;
import com.bank.kafka.common.event.BalanceSnapshot;
import com.bank.kafka.ledger.topology.LedgerTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * <b>Interactive Queries</b>: the state stores of the topology are queried directly over REST,
 * no external database needed.
 *
 * <p>State is sharded: each instance only holds the partitions (accounts) assigned to it. The
 * metadata API tells which instance ({@code application.server}) owns a key; if it is another
 * instance the request is forwarded there over HTTP.
 */
@Service
public class BalanceQueryService {

    private final StreamsBuilderFactoryBean factoryBean;
    private final HostInfo self;
    private final RestClient restClient = RestClient.create();

    public BalanceQueryService(StreamsBuilderFactoryBean factoryBean,
                               @Value("${banking.ledger.advertised-host:localhost}") String host,
                               @Value("${server.port}") int port) {
        this.factoryBean = factoryBean;
        this.self = new HostInfo(host, port);
    }

    public Optional<BalanceSnapshot> balance(String accountId) {
        KafkaStreams streams = streams();
        KeyQueryMetadata metadata = streams.queryMetadataForKey(LedgerTopology.LEDGER_STORE, accountId,
                Serdes.String().serializer());
        if (metadata == null || KeyQueryMetadata.NOT_AVAILABLE.equals(metadata)) {
            throw new IllegalStateException("Store not available yet (rebalancing), retry shortly");
        }
        if (!self.equals(metadata.activeHost())) {
            HostInfo owner = metadata.activeHost();
            return Optional.ofNullable(restClient.get()
                    .uri("http://{host}:{port}/api/balances/{id}", owner.host(), owner.port(), accountId)
                    .retrieve()
                    .body(BalanceSnapshot.class));
        }
        return Optional.ofNullable(ledgerStore().get(accountId));
    }

    /** All balances held by <i>this</i> instance. */
    public List<BalanceSnapshot> localBalances() {
        List<BalanceSnapshot> result = new ArrayList<>();
        try (KeyValueIterator<String, BalanceSnapshot> it = ledgerStore().all()) {
            it.forEachRemaining((KeyValue<String, BalanceSnapshot> kv) -> result.add(kv.value));
        }
        return result;
    }

    /** Windowed store query: in-progress (not yet suppressed) activity windows of one account. */
    public List<AccountActivitySummary> activity(String accountId, Duration lookBack) {
        ReadOnlyWindowStore<String, AccountActivitySummary> store = streams().store(StoreQueryParameters.fromNameAndType(
                LedgerTopology.ACTIVITY_STORE, QueryableStoreTypes.windowStore()));
        Instant now = Instant.now();
        List<AccountActivitySummary> result = new ArrayList<>();
        try (WindowStoreIterator<AccountActivitySummary> it = store.fetch(accountId, now.minus(lookBack), now)) {
            it.forEachRemaining(kv -> result.add(kv.value.withWindow(Instant.ofEpochMilli(kv.key), null)));
        }
        return result;
    }

    public String state() {
        return streams().state().name();
    }

    private ReadOnlyKeyValueStore<String, BalanceSnapshot> ledgerStore() {
        return streams().store(StoreQueryParameters.fromNameAndType(
                LedgerTopology.LEDGER_STORE, QueryableStoreTypes.keyValueStore()));
    }

    private KafkaStreams streams() {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null || !streams.state().isRunningOrRebalancing()) {
            throw new IllegalStateException("Kafka Streams is not running");
        }
        return streams;
    }
}
