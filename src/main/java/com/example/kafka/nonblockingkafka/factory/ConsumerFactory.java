package com.example.kafka.nonblockingkafka.factory;

import com.example.kafka.nonblockingkafka.consumer.CustomJsonDeserializer;
import com.example.kafka.nonblockingkafka.model.ExampleQueueMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class ConsumerFactory {
    private final String bootstrapServers;

    public ConsumerFactory(@Value("${bootstrap.server:localhost:9092}")
                                   String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public <T> KafkaReceiver<String, ExampleQueueMessage<T>> newReciever(String... topics) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomJsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ReceiverOptions<String, ExampleQueueMessage<T>> receiverOptions =
                ReceiverOptions.<String, ExampleQueueMessage<T>>create(props).subscription(Arrays.asList(topics))
                        .addAssignListener(partitions -> log.debug("onPartitionsAssigned {}", partitions))
                        .addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));
        return KafkaReceiver.create(receiverOptions);
    }
}
