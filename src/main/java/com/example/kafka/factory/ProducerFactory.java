package com.example.kafka.factory;

import com.example.kafka.model.ExampleQueueMessage;
import com.example.kafka.producer.CustomJsonSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.HashMap;
import java.util.Map;

@Component
public class ProducerFactory {
    private final String bootstrapServers;

    public ProducerFactory(@Value("${bootstrap.server:localhost:9092}")
                                   String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public <T> KafkaSender<String, ExampleQueueMessage<T>> newSender() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "cliente-id-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomJsonSerializer.class);
        SenderOptions<String, ExampleQueueMessage<T>> senderOptions = SenderOptions.create(props);
        return KafkaSender.create(senderOptions);
    }
}
