package com.bank.kafka.ledger.topology;

import com.bank.kafka.common.event.BalanceSnapshot;
import com.bank.kafka.common.event.TransactionEvent;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * <b>Processor API + state store</b>: the heart of the ledger.
 *
 * <p>The store holds the current balance per account. Since the input is partitioned by
 * accountId, all transactions of an account are handled by the same task, one at a time - no
 * locks needed. With {@code processing.guarantee=exactly_once_v2} the store update (via its
 * changelog topic), the output record and the input offset are committed atomically, so a
 * crash can never debit an account twice.
 */
public class LedgerProcessor implements FixedKeyProcessor<String, TransactionEvent, TransactionEvent> {

    private static final Logger log = LoggerFactory.getLogger(LedgerProcessor.class);

    private final String storeName;
    private final BigDecimal overdraftLimit;
    private FixedKeyProcessorContext<String, TransactionEvent> context;
    private KeyValueStore<String, BalanceSnapshot> balances;

    public LedgerProcessor(String storeName, BigDecimal overdraftLimit) {
        this.storeName = storeName;
        this.overdraftLimit = overdraftLimit;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, TransactionEvent> context) {
        this.context = context;
        this.balances = context.getStateStore(storeName);
    }

    @Override
    public void process(FixedKeyRecord<String, TransactionEvent> record) {
        TransactionEvent txn = record.value();
        if (txn == null) {
            return;
        }
        BalanceSnapshot current = balances.get(record.key());
        if (current == null) {
            current = BalanceSnapshot.opening(record.key(), txn.currency());
        }
        if (txn.transactionId().equals(current.lastTransactionId())) {
            log.warn("Transaction {} already applied to {}", txn.transactionId(), record.key());
            return;
        }

        BigDecimal newBalance = current.balance().add(txn.signedAmount());
        if (!txn.type().isCredit() && newBalance.compareTo(overdraftLimit.negate()) < 0) {
            context.forward(record.withValue(txn.rejected(
                    "INSUFFICIENT_FUNDS: available " + current.balance() + " " + current.currency())));
            return;
        }

        balances.put(record.key(), current.apply(txn, newBalance));
        context.forward(record.withValue(txn.posted(newBalance)));
    }
}
