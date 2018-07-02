package com.example.kafka.nonblockingkafka.factory;

import com.example.kafka.nonblockingkafka.model.ExampleQueueMessage;
import com.example.kafka.nonblockingkafka.model.UniqueKafka;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Collections;

@Component
public class ProducerRecordFactory {

    private static final String TYPE = "type";

    public <T> ProducerRecord<String, ExampleQueueMessage<T>> produceRecord(String topic, UniqueKafka body) {
        return produceRecord(topic, LocalDateTime.now(), 0, body);
    }

    @SuppressWarnings("unchecked")
    public <T> ProducerRecord<String, ExampleQueueMessage<T>> produceRecord(String topic, LocalDateTime timeToBeConsume, Integer retries, UniqueKafka body) {
        Class<T> type = (Class<T>) body.getClass();
        RecordHeader header = new RecordHeader(TYPE, type.getName().getBytes());
        ExampleQueueMessage<T> exampleMsg = ExampleQueueMessage
                .<T>builder()
                .body((T) body)
                .timeToBeConsume(timeToBeConsume)
                .key(body.getKey())
                .retries(retries)
                .build();
        return new ProducerRecord<>(
                topic,
                null,
                null,
                body.getKey(),
                exampleMsg,
                Collections.singletonList(header));
    }
}
