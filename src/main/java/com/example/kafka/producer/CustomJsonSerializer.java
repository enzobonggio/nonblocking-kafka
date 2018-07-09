package com.example.kafka.producer;

import com.example.kafka.model.ExampleQueueMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import reactor.core.Exceptions;

import java.util.Map;

public class CustomJsonSerializer<T> implements ExtendedSerializer<ExampleQueueMessage<T>> {
    @Override
    public byte[] serialize(String topic, Headers headers, ExampleQueueMessage<T> data) {
        return serialize(topic, data);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, ExampleQueueMessage<T> data) {
        try {
            final ObjectMapper mapper = new ObjectMapper()
                    .findAndRegisterModules();
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public void close() {

    }
}
