package com.example.kafka.nonblockingkafka.producer;

import com.example.kafka.nonblockingkafka.factory.ProducerFactory;
import com.example.kafka.nonblockingkafka.factory.ProducerRecordFactory;
import com.example.kafka.nonblockingkafka.model.ExampleBody;
import com.example.kafka.nonblockingkafka.model.ExampleQueueMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Slf4j
@RestController
public class ExampleProducer {
    private final String topic;
    private final KafkaSender<String, ExampleQueueMessage<ExampleBody>> sender;
    private final ProducerRecordFactory producerRecordFactory;


    public ExampleProducer(
            @Value("topic.name:demo-topic") String topic,
            ProducerFactory producerFactory,
            ProducerRecordFactory producerRecordFactory) {
        this.topic = topic;
        this.producerRecordFactory = producerRecordFactory;
        this.sender = producerFactory.newSender();
    }

    @PostMapping
    public Mono<Void> sendMessages(@RequestBody ExampleBody body) {
        ProducerRecord<String, ExampleQueueMessage<ExampleBody>> producerRecord =
                producerRecordFactory.produceRecord(topic, body);
        return sendMessages(producerRecord);
    }

    public Mono<Void> sendMessages(ProducerRecord<String, ExampleQueueMessage<ExampleBody>> producerRecord) {
        return sender.send(Mono.just(SenderRecord.create(producerRecord, "1")))
                .doOnError(e -> log.error("Send failed", e))
                .next().then();
    }

}
