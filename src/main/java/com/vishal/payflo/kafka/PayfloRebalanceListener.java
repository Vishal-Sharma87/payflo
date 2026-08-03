    package com.vishal.payflo.kafka;

    import lombok.extern.slf4j.Slf4j;
    import org.apache.kafka.clients.consumer.Consumer;
    import org.apache.kafka.common.TopicPartition;
    import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
    import org.springframework.stereotype.Component;

    import java.util.Collection;

    @Slf4j
    @Component
    public class PayfloRebalanceListener implements ConsumerAwareRebalanceListener {
        @Override
        public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            log.info("Partitions revoked: {}", partitions);
        }

        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            log.info("Partitions assigned: {}", partitions);
        }
    }