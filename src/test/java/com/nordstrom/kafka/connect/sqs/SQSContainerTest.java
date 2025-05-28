package com.nordstrom.kafka.connect.sqs;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class SQSContainerTest {
    private static Network network = Network.newNetwork();

    public static ConfluentKafkaContainer kafkaContainer = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.9.0")
            .withNetwork(network)
            .withEnv("KAFKA_LISTENERS", "PLAINTEXT://:9092,BROKER://:9093,CONTROLLER://:9094");

    public static LocalStackContainer localstackContainer = new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
            .withNetwork(network)
            .withServices(LocalStackContainer.Service.SQS)
            .withEnv("DEFAULT_REGION", "us-east-1");

    public static GenericContainer kafkaConnectContainer = new GenericContainer(DockerImageName.parse("confluentinc/cp-kafka-connect:7.9.0"))
            .withNetwork(network)
            .withEnv("CONNECT_BOOTSTRAP_SERVERS", "kafka:9092")
            .withEnv("CONNECT_REST_PORT", "8083")
            .withEnv("CONNECT_GROUP_ID", "connect-cluster")
            .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect-configs")
            .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offsets")
            .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect-statuses")
            .withExposedPorts(8083)
            .withLogConsumer(new Slf4jLogConsumer(org.slf4j.LoggerFactory.getLogger("kafka-connect")))
            .dependsOn(kafkaContainer);


    public static void setup() {
        kafkaContainer.start();
        localstackContainer.start();
        kafkaConnectContainer.start();

        // Additional setup for SQS, if needed
        String sqsEndpoint = localstackContainer.getEndpointOverride(LocalStackContainer.Service.SQS).toString();
        System.setProperty("sqs.endpoint", sqsEndpoint);

        // Optionally, create SQS queues or perform other setup tasks
    }

    public static void teardown() {
        if (kafkaConnectContainer.isRunning()) {
            kafkaConnectContainer.stop();
        }
        if (kafkaContainer.isRunning()) {
            kafkaContainer.stop();
        }
        if (localstackContainer.isRunning()) {
            localstackContainer.stop();
        }
    }
}
