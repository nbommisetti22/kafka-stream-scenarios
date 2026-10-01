package com.bank.kafka.common.serde;

import com.bank.kafka.common.event.AccountActivitySummary;
import com.bank.kafka.common.event.AccountEvent;
import com.bank.kafka.common.event.BalanceSnapshot;
import com.bank.kafka.common.event.CustomerProfile;
import com.bank.kafka.common.event.EnrichedTransaction;
import com.bank.kafka.common.event.FraudAlert;
import com.bank.kafka.common.event.TransactionEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/** Ready-made serdes for every event type, used by the Kafka Streams topologies. */
public final class BankSerdes {

    public static Serde<String> key() {
        return Serdes.String();
    }

    public static Serde<TransactionEvent> transaction() {
        return new JsonSerde<>(TransactionEvent.class);
    }

    public static Serde<AccountEvent> account() {
        return new JsonSerde<>(AccountEvent.class);
    }

    public static Serde<CustomerProfile> customer() {
        return new JsonSerde<>(CustomerProfile.class);
    }

    public static Serde<BalanceSnapshot> balance() {
        return new JsonSerde<>(BalanceSnapshot.class);
    }

    public static Serde<EnrichedTransaction> enriched() {
        return new JsonSerde<>(EnrichedTransaction.class);
    }

    public static Serde<FraudAlert> fraudAlert() {
        return new JsonSerde<>(FraudAlert.class);
    }

    public static Serde<AccountActivitySummary> activitySummary() {
        return new JsonSerde<>(AccountActivitySummary.class);
    }

    private BankSerdes() {
    }
}
