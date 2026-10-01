package com.bank.kafka.processor.validation;

/**
 * Transient: the account may exist but its AccountOpened event has not reached our local cache
 * yet (eventual consistency between topics). Retried by the error handler.
 */
public class AccountNotFoundException extends RuntimeException {

    public AccountNotFoundException(String accountId) {
        super("Account " + accountId + " not (yet) known");
    }
}
