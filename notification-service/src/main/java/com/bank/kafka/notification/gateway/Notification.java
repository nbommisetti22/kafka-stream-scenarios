package com.bank.kafka.notification.gateway;

import java.time.Instant;

public record Notification(String channel, String recipient, String accountId, String message, Instant sentAt) {
}
