package com.bank.kafka.notification.api;

import com.bank.kafka.notification.gateway.Notification;
import com.bank.kafka.notification.gateway.NotificationGateway;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    private final NotificationGateway gateway;

    public NotificationController(NotificationGateway gateway) {
        this.gateway = gateway;
    }

    @GetMapping
    public List<Notification> recent(@RequestParam(required = false) String accountId) {
        return gateway.recent().stream()
                .filter(n -> accountId == null || accountId.equals(n.accountId()))
                .toList();
    }
}
