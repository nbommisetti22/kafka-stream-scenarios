package com.bank.kafka.processor.validation;

import com.bank.kafka.common.event.AccountEvent;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory replica of the compacted {@code bank.accounts} topic. */
@Component
public class AccountCache {

    private final Map<String, AccountEvent> accounts = new ConcurrentHashMap<>();

    public void put(AccountEvent account) {
        accounts.put(account.accountId(), account);
    }

    public void remove(String accountId) {
        accounts.remove(accountId);
    }

    public Optional<AccountEvent> get(String accountId) {
        return Optional.ofNullable(accounts.get(accountId));
    }

    public int size() {
        return accounts.size();
    }
}
