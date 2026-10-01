package com.bank.kafka.notification.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Simulated SMS / e-mail / push provider. It can be configured to fail randomly to demonstrate
 * retry topics. Sent notifications are kept in memory and exposed over REST.
 */
@Component
public class NotificationGateway {

    private static final Logger log = LoggerFactory.getLogger(NotificationGateway.class);
    private static final int MAX_KEPT = 500;

    private final double failureRate;
    private final Deque<Notification> sent = new ConcurrentLinkedDeque<>();

    public NotificationGateway(@Value("${banking.notifications.simulated-failure-rate:0.0}") double failureRate) {
        this.failureRate = failureRate;
    }

    public void send(String channel, String recipient, String accountId, String message) {
        if (failureRate > 0 && ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new GatewayUnavailableException(channel + " provider timed out");
        }
        Notification n = new Notification(channel, recipient, accountId, message, Instant.now());
        sent.addFirst(n);
        while (sent.size() > MAX_KEPT) {
            sent.pollLast();
        }
        log.info("[{}] -> {}: {}", channel, recipient, message);
    }

    public List<Notification> recent() {
        return Collections.unmodifiableList(new ArrayList<>(sent));
    }
}
