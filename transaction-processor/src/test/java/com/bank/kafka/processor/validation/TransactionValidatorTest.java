package com.bank.kafka.processor.validation;

import com.bank.kafka.common.event.AccountEvent;
import com.bank.kafka.common.event.AccountStatus;
import com.bank.kafka.common.event.TransactionEvent;
import com.bank.kafka.common.event.TransactionType;
import com.bank.kafka.processor.config.ProcessorProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionValidatorTest {

    private final AccountCache cache = new AccountCache();
    private TransactionValidator validator;

    @BeforeEach
    void setUp() {
        ProcessorProperties props = new ProcessorProperties(3, Duration.ofMillis(100), ".DLT", 100,
                Set.of("USD", "EUR"), new BigDecimal("50000"), Map.of("ATM", new BigDecimal("2000")));
        validator = new TransactionValidator(cache, props);
        cache.put(account("ACC-1", "USD", AccountStatus.ACTIVE));
        cache.put(account("ACC-2", "USD", AccountStatus.ACTIVE));
        cache.put(account("ACC-EUR", "EUR", AccountStatus.ACTIVE));
        cache.put(account("ACC-FROZEN", "USD", AccountStatus.FROZEN));
    }

    @Test
    void validDeposit() {
        assertThat(validator.validate(txn("ACC-1", TransactionType.DEPOSIT, "100.25", "USD", "ONLINE")).valid())
                .isTrue();
    }

    @Test
    void rejectsNonPositiveAndTooPreciseAmounts() {
        assertThat(validator.validate(txn("ACC-1", TransactionType.DEPOSIT, "0", "USD", "ONLINE")).reason())
                .startsWith("INVALID_AMOUNT");
        assertThat(validator.validate(txn("ACC-1", TransactionType.DEPOSIT, "1.001", "USD", "ONLINE")).reason())
                .startsWith("INVALID_AMOUNT");
    }

    @Test
    void rejectsUnsupportedCurrencyAndMismatch() {
        assertThat(validator.validate(txn("ACC-1", TransactionType.DEPOSIT, "10", "JPY", "ONLINE")).reason())
                .startsWith("UNSUPPORTED_CURRENCY");
        assertThat(validator.validate(txn("ACC-EUR", TransactionType.DEPOSIT, "10", "USD", "ONLINE")).reason())
                .startsWith("CURRENCY_MISMATCH");
    }

    @Test
    void appliesChannelLimits() {
        assertThat(validator.validate(txn("ACC-1", TransactionType.WITHDRAWAL, "2500", "USD", "ATM")).reason())
                .startsWith("LIMIT_EXCEEDED");
        assertThat(validator.validate(txn("ACC-1", TransactionType.WITHDRAWAL, "2500", "USD", "BRANCH")).valid())
                .isTrue();
    }

    @Test
    void rejectsFrozenAccount() {
        assertThat(validator.validate(txn("ACC-FROZEN", TransactionType.PAYMENT, "10", "USD", "ONLINE")).reason())
                .isEqualTo("ACCOUNT_FROZEN: ACC-FROZEN");
    }

    @Test
    void unknownAccountIsTransientError() {
        assertThatThrownBy(() -> validator.validate(txn("ACC-404", TransactionType.DEPOSIT, "10", "USD", "ONLINE")))
                .isInstanceOf(AccountNotFoundException.class);
    }

    @Test
    void validatesTransferCounterparty() {
        TransactionEvent ok = txn("ACC-1", TransactionType.TRANSFER, "10", "USD", "ONLINE").withCounterparty("ACC-2");
        assertThat(validator.validate(ok).valid()).isTrue();

        TransactionEvent self = txn("ACC-1", TransactionType.TRANSFER, "10", "USD", "ONLINE").withCounterparty("ACC-1");
        assertThat(validator.validate(self).reason()).startsWith("INVALID_TRANSFER");

        TransactionEvent frozen = txn("ACC-1", TransactionType.TRANSFER, "10", "USD", "ONLINE")
                .withCounterparty("ACC-FROZEN");
        assertThat(validator.validate(frozen).reason()).startsWith("COUNTERPARTY_ACCOUNT_FROZEN");
    }

    private static AccountEvent account(String id, String currency, AccountStatus status) {
        return new AccountEvent(id, "CUST-1", "CHECKING", currency, status, Instant.now());
    }

    private static TransactionEvent txn(String account, TransactionType type, String amount, String currency,
                                        String channel) {
        return TransactionEvent.request(account, type, new BigDecimal(amount), currency, channel, "US", null);
    }
}
