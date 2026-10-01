package com.bank.kafka.notification.listener;

import com.bank.kafka.common.Topics;
import com.bank.kafka.common.event.AccountActivitySummary;
import com.bank.kafka.notification.gateway.NotificationGateway;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Sends the periodic activity digest produced by the ledger's windowed aggregation. */
@Component
public class ActivitySummaryListener {

    private final NotificationGateway gateway;

    public ActivitySummaryListener(NotificationGateway gateway) {
        this.gateway = gateway;
    }

    @KafkaListener(
            id = "activity-digest",
            topics = Topics.ACCOUNT_ACTIVITY_SUMMARY,
            properties = "spring.json.value.default.type=com.bank.kafka.common.event.AccountActivitySummary")
    public void onSummary(AccountActivitySummary summary) {
        gateway.send("EMAIL", "owner-of-" + summary.accountId(), summary.accountId(),
                String.format("Activity %s - %s: %d transactions, in %s, out %s, net %s",
                        summary.windowStart(), summary.windowEnd(), summary.transactionCount(),
                        summary.totalCredits(), summary.totalDebits(), summary.netFlow()));
    }
}
