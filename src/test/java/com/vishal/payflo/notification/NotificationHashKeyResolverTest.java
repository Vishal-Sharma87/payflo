package com.vishal.payflo.notification;

import com.vishal.payflo.configs.NotificationHashKeyProperties;
import com.vishal.payflo.kafka.topics.KafkaTopic;
import com.vishal.payflo.notifications.NotificationHashKeyResolver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class NotificationHashKeyResolverTest {
    private static final String paymentInitiatedNotification = "notification:payment-initiated";
    private static final String paymentCompletedNotification = "notification:payment-completed";
    private static final String paymentFailedNotification = "notification:payment-failed";
    private static final String paymentTimedOutNotification = "notification:payment-timed-out";

    private final NotificationHashKeyProperties hashKeyProperties =
            new NotificationHashKeyProperties(
                    paymentInitiatedNotification,
                    paymentCompletedNotification,
                    paymentFailedNotification,
                    paymentTimedOutNotification);

    private final NotificationHashKeyResolver notificationHashKeyResolver = new NotificationHashKeyResolver(hashKeyProperties);

    @ParameterizedTest
    @MethodSource("validTopicsToHashKeys")
    public void testResolveWithValidTopics(KafkaTopic topic, String expectedHashKey){
        String actual = notificationHashKeyResolver.resolve(topic);
        Assertions.assertEquals(expectedHashKey, actual);
    }

    public Stream<Arguments> validTopicsToHashKeys() {
        return Stream.of(
                Arguments.of(KafkaTopic.PAYMENT_INITIATED_NOTIFICATION, paymentInitiatedNotification),
                Arguments.of(KafkaTopic.PAYMENT_COMPLETED_NOTIFICATION, paymentCompletedNotification),
                Arguments.of(KafkaTopic.PAYMENT_FAILED_NOTIFICATION, paymentFailedNotification),
                Arguments.of(KafkaTopic.PAYMENT_TIMED_OUT_NOTIFICATION, paymentTimedOutNotification)
        );
    }

    @ParameterizedTest
    @MethodSource("invalidTopicsThrowsException")
    public void testResolveWithInvalidTopics(KafkaTopic topic){
        Assertions.assertThrows(IllegalArgumentException.class, () -> notificationHashKeyResolver.resolve(topic));
    }

    public Stream<Arguments> invalidTopicsThrowsException() {
        return Stream.of(
                Arguments.of(KafkaTopic.PAYMENT_INITIATED),
                Arguments.of(KafkaTopic.PAYMENT_RECEIVED),
                Arguments.of(KafkaTopic.PAYMENT_FAILED),
                Arguments.of(KafkaTopic.PAYMENT_TIMED_OUT),
                Arguments.of(KafkaTopic.DLT)
        );
    }
}
