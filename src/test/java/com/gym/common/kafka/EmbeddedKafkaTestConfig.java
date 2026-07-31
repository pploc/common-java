package com.gym.common.kafka;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.test.context.EmbeddedKafka;

@TestConfiguration
@EmbeddedKafka(partitions = 1, topics = { "test-topic" })
public class EmbeddedKafkaTestConfig {
}
