package com.vishal.payflo.notification;

import com.vishal.payflo.cache.service.RedisHashService;
import com.vishal.payflo.kafka.topics.KafkaTopic;
import com.vishal.payflo.notifications.NotificationPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.UUID;

@ExtendWith(MockitoExtension.class)
public class NotificationPublisherTest {

    private static final UUID TRANSACTION_ID = UUID.randomUUID();
    private static final KafkaTopic KAFKA_TOPIC = KafkaTopic.PAYMENT_INITIATED;
    private static final String NOTIFICATION_PAYLOAD = "dummyContent";


    @Mock
    private RedisHashService hashService;

    private NotificationPublisher publisher;


    @BeforeEach
    public void setup(){
        publisher = new NotificationPublisher(hashService);
    }

    @Test
    public void testFirstTimeNotificationFiring(){
        Mockito.when(hashService.isNotificationSent(TRANSACTION_ID, KAFKA_TOPIC)).thenReturn(false);

        publisher.publish(TRANSACTION_ID, NOTIFICATION_PAYLOAD, KAFKA_TOPIC);

        Mockito.verify(hashService).notificationProcessed(TRANSACTION_ID, KAFKA_TOPIC);
    }

    @Test
    public void testDuplicateNotificationFireDiscard(){
        Mockito.when(hashService.isNotificationSent(TRANSACTION_ID, KAFKA_TOPIC)).thenReturn(true);

        publisher.publish(TRANSACTION_ID, NOTIFICATION_PAYLOAD, KAFKA_TOPIC);

        Mockito.verify(hashService, Mockito.never()).notificationProcessed(TRANSACTION_ID, KAFKA_TOPIC);
    }


}
