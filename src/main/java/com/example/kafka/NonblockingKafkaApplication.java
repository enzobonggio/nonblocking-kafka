package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan({"com.example.kafka", "com.rfsc.*"})
public class NonblockingKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(NonblockingKafkaApplication.class, args);
    }
}
