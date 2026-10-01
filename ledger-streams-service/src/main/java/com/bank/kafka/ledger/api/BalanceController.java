package com.bank.kafka.ledger.api;

import com.bank.kafka.common.event.AccountActivitySummary;
import com.bank.kafka.common.event.BalanceSnapshot;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class BalanceController {

    private final BalanceQueryService queries;

    public BalanceController(BalanceQueryService queries) {
        this.queries = queries;
    }

    @GetMapping("/balances/{accountId}")
    public ResponseEntity<BalanceSnapshot> balance(@PathVariable String accountId) {
        return ResponseEntity.of(queries.balance(accountId));
    }

    @GetMapping("/balances")
    public List<BalanceSnapshot> localBalances() {
        return queries.localBalances();
    }

    @GetMapping("/accounts/{accountId}/activity")
    public List<AccountActivitySummary> activity(@PathVariable String accountId,
                                                 @RequestParam(defaultValue = "PT24H") Duration lookBack) {
        return queries.activity(accountId, lookBack);
    }

    @GetMapping("/streams/state")
    public Map<String, String> state() {
        return Map.of("state", queries.state());
    }

    @ExceptionHandler({IllegalStateException.class, InvalidStateStoreException.class})
    public ProblemDetail unavailable(RuntimeException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }
}
