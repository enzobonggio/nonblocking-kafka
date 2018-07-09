package com.example.kafka.factory;

import com.example.kafka.consumer.CustomJsonDeserializer;
import com.example.kafka.model.ExampleQueueMessage;
import com.example.kafka.service.OffsetService;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
@Slf4j
public class ConsumerFactory {
    private final String bootstrapServers;

    private final OffsetService offsetService;

    public ConsumerFactory(@Value("${bootstrap.server:localhost:9092}")
                                   String bootstrapServers,
                           OffsetService offsetService) {
        this.bootstrapServers = bootstrapServers;
        this.offsetService = offsetService;
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

    public <T> KafkaReceiver<String, ExampleQueueMessage<T>> newRecieverWithControlledOffset(String... topics) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomJsonDeserializer.class);
        ReceiverOptions<String, ExampleQueueMessage<T>> receiverOptions =
                ReceiverOptions.<String, ExampleQueueMessage<T>>create(props).subscription(Arrays.asList(topics))
                        .addAssignListener(partitions -> {
                            log.debug("onPartitionsAssigned {}", partitions);
                            partitions.stream()
                                    .map(p -> Tuples.of(p, getOffset(p)))
                                    .forEach(this::setOffset);
                        })
                        .addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));
        return KafkaReceiver.create(receiverOptions);
    }

    private void setOffset(Tuple2<ReceiverPartition, Optional<Long>> tPartitionOffset) {
        val oOffset = tPartitionOffset.getT2();
        val partition = tPartitionOffset.getT1();
        if (oOffset.isPresent()) {
            partition.seek(oOffset.get());
        } else {
            partition.seekToBeginning();
        }
    }

    private Optional<Long> getOffset(ReceiverPartition p) {
        val partitionInfo = p.topicPartition();
        val topic = partitionInfo.topic();
        val partition = partitionInfo.partition();
        log.info("Calling offset service with topic {} and partition {}", topic, partition);
        return offsetService.getOptional(topic, partition);
    }
}
