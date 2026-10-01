package com.bank.kafka.account.service;

import com.bank.kafka.account.api.Requests.CreateAccountRequest;
import com.bank.kafka.account.api.Requests.CreateCustomerRequest;
import com.bank.kafka.account.api.Requests.TransactionRequest;
import com.bank.kafka.account.api.Requests.TransferRequest;
import com.bank.kafka.account.api.Responses.PublishedEvent;
import com.bank.kafka.common.Topics;
import com.bank.kafka.common.event.AccountEvent;
import com.bank.kafka.common.event.AccountStatus;
import com.bank.kafka.common.event.CustomerProfile;
import com.bank.kafka.common.event.TransactionEvent;
import com.bank.kafka.common.event.TransactionType;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class BankingCommandService {

    private final EventPublisher publisher;
    private final AccountRegistry registry;

    public BankingCommandService(EventPublisher publisher, AccountRegistry registry) {
        this.publisher = publisher;
        this.registry = registry;
    }

    public CompletableFuture<PublishedEvent> createCustomer(CreateCustomerRequest req) {
        String id = req.customerId() != null ? req.customerId() : "CUST-" + shortId();
        CustomerProfile profile = new CustomerProfile(id, req.fullName(), req.email(), req.phone(),
                req.homeCountry(), req.tier() != null ? req.tier() : "STANDARD");
        return publisher.publish(Topics.CUSTOMERS, id, profile, "CustomerUpserted")
                .thenApply(r -> PublishedEvent.of(id, r));
    }

    /** Publishes a tombstone (null value). Log compaction eventually removes all data for the key. */
    public CompletableFuture<PublishedEvent> deleteCustomer(String customerId) {
        return publisher.publish(Topics.CUSTOMERS, customerId, null, "CustomerDeleted")
                .thenApply(r -> PublishedEvent.of(customerId, r));
    }

    public CompletableFuture<PublishedEvent> openAccount(CreateAccountRequest req) {
        String id = req.accountId() != null ? req.accountId() : "ACC-" + shortId();
        AccountEvent account = new AccountEvent(id, req.customerId(), req.accountType(), req.currency(),
                AccountStatus.ACTIVE, Instant.now());
        registry.save(account);
        return publisher.publish(Topics.ACCOUNTS, id, account, "AccountOpened")
                .thenApply(r -> PublishedEvent.of(id, r));
    }

    public CompletableFuture<PublishedEvent> changeStatus(String accountId, AccountStatus status) {
        AccountEvent updated = registry.find(accountId)
                .orElseThrow(() -> new NoSuchElementException("Unknown account " + accountId))
                .withStatus(status);
        registry.save(updated);
        return publisher.publish(Topics.ACCOUNTS, accountId, updated, "AccountStatusChanged")
                .thenApply(r -> PublishedEvent.of(accountId, r));
    }

    /**
     * Keyed by accountId: every transaction of one account lands in the same partition and is
     * therefore processed strictly in order by downstream consumers.
     */
    public CompletableFuture<PublishedEvent> submitTransaction(TransactionRequest req) {
        TransactionEvent txn = toEvent(req);
        return publisher.publish(Topics.TRANSACTIONS_REQUESTED, txn.accountId(), txn, "TransactionRequested")
                .thenApply(r -> PublishedEvent.of(txn.transactionId(), r));
    }

    public CompletableFuture<PublishedEvent> submitTransfer(TransferRequest req) {
        TransactionEvent txn = TransactionEvent.request(req.fromAccountId(), TransactionType.TRANSFER, req.amount(),
                        req.currency(), req.channel() != null ? req.channel() : "ONLINE", req.country(), null)
                .withCounterparty(req.toAccountId());
        if (req.transactionId() != null) {
            txn = txn.withTransactionId(req.transactionId());
        }
        TransactionEvent event = txn;
        return publisher.publish(Topics.TRANSACTIONS_REQUESTED, event.accountId(), event, "TransferRequested")
                .thenApply(r -> PublishedEvent.of(event.transactionId(), r));
    }

    /** Bulk payment file (e.g. payroll): all-or-nothing using a Kafka transaction. */
    public List<PublishedEvent> submitBatch(List<TransactionRequest> requests) {
        List<TransactionEvent> events = requests.stream().map(BankingCommandService::toEvent).toList();
        List<EventPublisher.KeyedEvent> keyed = events.stream()
                .map(e -> new EventPublisher.KeyedEvent(e.accountId(), e))
                .toList();
        List<SendResult<String, Object>> results =
                publisher.publishAtomically(Topics.TRANSACTIONS_REQUESTED, keyed, "TransactionRequested");
        return java.util.stream.IntStream.range(0, results.size())
                .mapToObj(i -> PublishedEvent.of(events.get(i).transactionId(), results.get(i)))
                .toList();
    }

    private static TransactionEvent toEvent(TransactionRequest req) {
        TransactionEvent txn = TransactionEvent.request(req.accountId(), req.type(), req.amount(), req.currency(),
                req.channel() != null ? req.channel() : "ONLINE", req.country(), req.merchant());
        // Client supplied id = idempotency key: resubmitting the same request is de-duplicated downstream.
        return req.transactionId() != null ? txn.withTransactionId(req.transactionId()) : txn;
    }

    private static String shortId() {
        return UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }
}
