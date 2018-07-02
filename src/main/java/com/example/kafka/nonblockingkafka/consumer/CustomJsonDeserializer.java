package com.example.kafka.nonblockingkafka.consumer;

import com.example.kafka.nonblockingkafka.model.ExampleQueueMessage;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedDeserializer;
import reactor.core.Exceptions;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.StreamSupport;

public class CustomJsonDeserializer<T> implements ExtendedDeserializer<ExampleQueueMessage<T>> {

    private static final String TYPE = "type";
    private Class<T> targetType;

    @SuppressWarnings("unchecked")
    @Override
    public ExampleQueueMessage<T> deserialize(String topic, Headers headers, byte[] data) {
        targetType = (Class<T>) StreamSupport.stream(headers.spliterator(), false)
                .filter(h -> TYPE.equals(h.key()))
                .map(Header::value)
                .map(String::new)
                .map(s -> {
                    try {
                        return Class.forName(s);
                    } catch (ClassNotFoundException e) {
                        throw Exceptions.propagate(e);
                    }
                })
                .findFirst().orElse(null);
        return deserialize(topic, data);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public ExampleQueueMessage<T> deserialize(String topic, byte[] data) {
        Objects.requireNonNull(targetType, "Need a targetType to convert send it on the header");
        final ObjectMapper mapper = new ObjectMapper()
                .findAndRegisterModules();
        JavaType queueMessageType = mapper.getTypeFactory().constructParametricType(ExampleQueueMessage.class, targetType);
        try {
            return mapper.readValue(data, queueMessageType);
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public void close() {

    }
}
