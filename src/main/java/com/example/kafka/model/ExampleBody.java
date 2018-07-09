package com.example.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ExampleBody implements UniqueKafka {
    String value;

    @Override
    @JsonIgnore
    public String getKey() {
        return value;
    }
}
