package com.bank.kafka.ledger.topology;

import com.bank.kafka.common.event.BalanceSnapshot;
import com.bank.kafka.common.event.TransactionEvent;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

/** Turns a posted transaction into the account's current {@link BalanceSnapshot} from the ledger store. */
public class BalanceLookupProcessor implements FixedKeyProcessor<String, TransactionEvent, BalanceSnapshot> {

    private final String storeName;
    private FixedKeyProcessorContext<String, BalanceSnapshot> context;
    private KeyValueStore<String, BalanceSnapshot> balances;

    public BalanceLookupProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, BalanceSnapshot> context) {
        this.context = context;
        this.balances = context.getStateStore(storeName);
    }

    @Override
    public void process(FixedKeyRecord<String, TransactionEvent> record) {
        BalanceSnapshot snapshot = balances.get(record.key());
        if (snapshot != null) {
            context.forward(record.withValue(snapshot));
        }
    }
}
