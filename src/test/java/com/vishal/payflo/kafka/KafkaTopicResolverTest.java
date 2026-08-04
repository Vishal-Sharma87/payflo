package com.vishal.payflo.kafka;

import com.vishal.payflo.kafka.topics.KafkaTopic;
import com.vishal.payflo.kafka.topics.KafkaTopicResolver;
import com.vishal.payflo.kafka.topics.KafkaTopicsProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class KafkaTopicResolverTest {

    private static final String paymentInitiated = "payflo.payment-initiated";
    private static final String notificationPaymentInitiated = "payflo.notification.payment-initiated";
    private static final String paymentReceived = "payflo.payment-received";
    private static final String notificationPaymentCompleted = "payflo.notification.payment-completed";
    private static final String paymentFailed = "payflo.payment-failed";
    private static final String notificationPaymentFailed = "payflo.notification.payment-failed";
    private static final String paymentTimedOut = "payflo.payment-timed-out";
    private static final String notificationPaymentTimedOut = "payflo.notification.payment-timed-out";
    private static final String dlt = "payflo.DLT";

    private final KafkaTopicsProperties kafkaTopicsProperties = new KafkaTopicsProperties(paymentInitiated, notificationPaymentInitiated, paymentReceived, notificationPaymentCompleted, paymentFailed, notificationPaymentFailed, paymentTimedOut, notificationPaymentTimedOut, dlt);

    private final KafkaTopicResolver kafkaTopicResolver = new KafkaTopicResolver(kafkaTopicsProperties);



    public static Stream<Arguments> kafkaTopicEnumsToKafkaTopicTopicNameStream() {
        return Stream.of
                (
                        Arguments.of(KafkaTopic.PAYMENT_INITIATED, paymentInitiated),
                        Arguments.of(KafkaTopic.PAYMENT_INITIATED_NOTIFICATION, notificationPaymentInitiated),
                        Arguments.of(KafkaTopic.PAYMENT_RECEIVED, paymentReceived),
                        Arguments.of(KafkaTopic.PAYMENT_COMPLETED_NOTIFICATION, notificationPaymentCompleted),
                        Arguments.of(KafkaTopic.PAYMENT_FAILED, paymentFailed),
                        Arguments.of(KafkaTopic.PAYMENT_FAILED_NOTIFICATION, notificationPaymentFailed),
                        Arguments.of(KafkaTopic.PAYMENT_TIMED_OUT, paymentTimedOut),
                        Arguments.of(KafkaTopic.PAYMENT_TIMED_OUT_NOTIFICATION, notificationPaymentTimedOut),
                        Arguments.of(KafkaTopic.DLT, dlt)
                );
    }


    @ParameterizedTest
    @MethodSource("kafkaTopicEnumsToKafkaTopicTopicNameStream")
    public void testResolver(KafkaTopic topic, String expectedKafkaTopicName) {
        String actualTopicName = kafkaTopicResolver.resolve(topic);

        Assertions.assertEquals(expectedKafkaTopicName, actualTopicName);
    }


}
