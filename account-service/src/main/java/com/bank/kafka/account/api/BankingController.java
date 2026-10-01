package com.bank.kafka.account.api;

import com.bank.kafka.account.api.Requests.CreateAccountRequest;
import com.bank.kafka.account.api.Requests.CreateCustomerRequest;
import com.bank.kafka.account.api.Requests.TransactionRequest;
import com.bank.kafka.account.api.Requests.TransferRequest;
import com.bank.kafka.account.api.Responses.PublishedEvent;
import com.bank.kafka.account.service.BankingCommandService;
import com.bank.kafka.common.event.AccountStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * Command side of the bank. Every endpoint only publishes an event and returns
 * {@code 202 Accepted}: the actual processing happens asynchronously downstream.
 */
@RestController
@RequestMapping("/api")
public class BankingController {

    private final BankingCommandService service;

    public BankingController(BankingCommandService service) {
        this.service = service;
    }

    @PostMapping("/customers")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CompletableFuture<PublishedEvent> createCustomer(@Valid @RequestBody CreateCustomerRequest request) {
        return service.createCustomer(request);
    }

    @DeleteMapping("/customers/{customerId}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CompletableFuture<PublishedEvent> deleteCustomer(@PathVariable String customerId) {
        return service.deleteCustomer(customerId);
    }

    @PostMapping("/accounts")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CompletableFuture<PublishedEvent> openAccount(@Valid @RequestBody CreateAccountRequest request) {
        return service.openAccount(request);
    }

    @PostMapping("/accounts/{accountId}/freeze")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CompletableFuture<PublishedEvent> freeze(@PathVariable String accountId) {
        return service.changeStatus(accountId, AccountStatus.FROZEN);
    }

    @PostMapping("/accounts/{accountId}/unfreeze")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CompletableFuture<PublishedEvent> unfreeze(@PathVariable String accountId) {
        return service.changeStatus(accountId, AccountStatus.ACTIVE);
    }

    @PostMapping("/transactions")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CompletableFuture<PublishedEvent> submit(@Valid @RequestBody TransactionRequest request) {
        return service.submitTransaction(request);
    }

    @PostMapping("/transfers")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CompletableFuture<PublishedEvent> transfer(@Valid @RequestBody TransferRequest request) {
        return service.submitTransfer(request);
    }

    @PostMapping("/transactions/batch")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public List<PublishedEvent> batch(@RequestBody @NotEmpty List<@Valid TransactionRequest> requests) {
        return service.submitBatch(requests);
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ProblemDetail notFound(NoSuchElementException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }
}
