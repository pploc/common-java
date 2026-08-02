package com.gym.common.testutil;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration
@Testcontainers
public class TestContainersConfig {

    private static final String KAFKA_ALIAS = "kafka";
    private static final int SCHEMA_REGISTRY_PORT = 8081;

    private static final Network network = Network.newNetwork();

    @Container
    public static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16"))
            .withDatabaseName("test_db")
            .withUsername("test_user")
            .withPassword("test_pass");

    @Container
    public static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"))
            .withNetwork(network)
            .withNetworkAliases(KAFKA_ALIAS);

    @Container
    public static final GenericContainer<?> schemaRegistry = new GenericContainer<>(
            DockerImageName.parse("confluentinc/cp-schema-registry:7.7.1")
    )
            .withNetwork(network)
            .withExposedPorts(SCHEMA_REGISTRY_PORT)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + KAFKA_ALIAS + ":9092")
            .dependsOn(kafka);

    static {
        postgres.start();
        kafka.start();
        schemaRegistry.start();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("gym.kafka.schema-registry-url", TestContainersConfig::schemaRegistryUrl);
    }

    public static String schemaRegistryUrl() {
        return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(SCHEMA_REGISTRY_PORT);
    }
}
