package com.bank.kafka.account.service;

import com.bank.kafka.common.event.AccountEvent;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local view of the accounts this service created, so that state changes (freeze/close) can
 * republish the full entity to the compacted topic. A production service would use its own
 * database plus the transactional outbox pattern.
 */
@Component
public class AccountRegistry {

    private final Map<String, AccountEvent> accounts = new ConcurrentHashMap<>();

    public void save(AccountEvent account) {
        accounts.put(account.accountId(), account);
    }

    public Optional<AccountEvent> find(String accountId) {
        return Optional.ofNullable(accounts.get(accountId));
    }
}
