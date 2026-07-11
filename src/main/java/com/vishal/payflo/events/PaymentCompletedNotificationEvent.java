package com.vishal.payflo.events;

import java.util.UUID;

public record PaymentCompletedNotificationEvent(UUID transactionId, String payload) {
}
