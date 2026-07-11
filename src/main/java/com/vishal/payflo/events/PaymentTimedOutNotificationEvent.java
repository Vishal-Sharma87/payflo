package com.vishal.payflo.events;

import java.util.UUID;

public record PaymentTimedOutNotificationEvent(UUID transactionId, String payload) {
}
