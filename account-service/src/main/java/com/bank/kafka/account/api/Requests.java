package com.bank.kafka.account.api;

import com.bank.kafka.common.event.TransactionType;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public final class Requests {

    public record CreateCustomerRequest(
            String customerId,
            @NotBlank String fullName,
            @Email String email,
            String phone,
            @NotBlank String homeCountry,
            String tier) {
    }

    public record CreateAccountRequest(
            String accountId,
            @NotBlank String customerId,
            @NotBlank String accountType,
            @NotBlank String currency) {
    }

    public record TransactionRequest(
            String transactionId,
            @NotBlank String accountId,
            @NotNull TransactionType type,
            @NotNull @DecimalMin("0.01") BigDecimal amount,
            @NotBlank String currency,
            String channel,
            String country,
            String merchant) {
    }

    public record TransferRequest(
            String transactionId,
            @NotBlank String fromAccountId,
            @NotBlank String toAccountId,
            @NotNull @DecimalMin("0.01") BigDecimal amount,
            @NotBlank String currency,
            String channel,
            String country) {
    }

    private Requests() {
    }
}
