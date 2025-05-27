package com.example.connector;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
// S3 specific imports removed: AwsBasicCredentials, StaticCredentialsProvider, Region, S3Client, CreateBucketRequest, HeadBucketRequest, NoSuchBucketException

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class KafkaConnectorIntegrationTest {

    private static final DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:0.14.0");
    private static final DockerImageName kafkaImage = DockerImageName.parse("confluentinc/cp-kafka:7.3.0");
    private static final DockerImageName kafkaConnectImage = DockerImageName.parse("confluentinc/cp-kafka-connect:7.3.0");

    // DLQ_BUCKET_NAME removed
    private static final String INPUT_TOPIC = "input-topic";
    private static final String DLQ_KAFKA_TOPIC = "dlq-kafka-topic";
    private static final String CONNECTOR_NAME = "test-sqs-dlq-sink"; // Renamed for SQS

    private static Network network = Network.newNetwork();

    private static LocalStackContainer localstack = new LocalStackContainer(localstackImage)
            .withServices(LocalStackContainer.Service.SQS) // Changed from S3 to SQS
            .withNetwork(network)
            .withNetworkAliases("localstack");

    private static KafkaContainer kafka = new KafkaContainer(kafkaImage)
            .withNetwork(network)
            .withNetworkAliases("kafka");

    @SuppressWarnings("rawtypes") // Testcontainers GenericContainer often used without type params
    private static GenericContainer kafkaConnect = new GenericContainer<>(kafkaConnectImage)
            .withExposedPorts(8083)
            .withNetwork(network)
            .withNetworkAliases("connect")
            // Initial placeholder for bootstrap servers, correctly set in startContainers
            .withEnv("CONNECT_BOOTSTRAP_SERVERS", "kafka:9092") // Using network alias
            .withEnv("CONNECT_GROUP_ID", "test-connect-group")
            .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "test-connect-configs")
            .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "test-connect-offsets")
            .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "test-connect-status")
            .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
            .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
            .withEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect") // Using network alias
            .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/usr/share/confluent-hub-components")
            .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO")
            .dependsOn(kafka); // Ensures Kafka is started before Kafka Connect

    // s3Client field removed
    private static HttpClient httpClient = HttpClient.newHttpClient();

    @BeforeAll
    static void startContainers() {
        localstack.start();
        kafka.start();
        // Update CONNECT_BOOTSTRAP_SERVERS with the actual bootstrap server from the Kafka container
        // Although we use network alias, Kafka container might expose on a different internal port sometimes,
        // getBootstrapServers() is safer.
        kafkaConnect.withEnv("CONNECT_BOOTSTRAP_SERVERS", kafka.getBootstrapServers().replace("localhost", "kafka"));
        kafkaConnect.start();
    }

    @AfterAll
    static void stopContainers() {
        if (kafkaConnect != null) {
            kafkaConnect.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
        if (localstack != null) {
            localstack.stop();
        }
    }

    @BeforeEach
    void setUpEnvironment() {
        // S3 client initialization and bucket creation logic removed.
        // This method can be used for other environment setup if needed in the future.
        // For now, it's empty.
    }

    private void deployConnectorConfig(String connectorJsonConfig) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + kafkaConnect.getMappedPort(8083) + "/connectors"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(connectorJsonConfig))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        // Basic check, can be enhanced to check status code 201 and response body
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Failed to deploy connector. Status: " + response.statusCode() + " Body: " + response.body());
        }
        System.out.println("Connector deployment response: " + response.body());
    }

    @Test
    void testSQSSinkErrorRoutesToKafkaDlq() throws Exception { // Renamed test method
        // 1. Create Kafka Topics
        try (AdminClient adminClient = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            adminClient.createTopics(Collections.singleton(new NewTopic(INPUT_TOPIC, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
            adminClient.createTopics(Collections.singleton(new NewTopic(DLQ_KAFKA_TOPIC, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
            System.out.println("Topics created: " + INPUT_TOPIC + ", " + DLQ_KAFKA_TOPIC);
        }

        // 2. Deploy Sink Connector Configuration (FileStreamSinkConnector that will fail)
        String connectorConfigJson = String.format(
            "{\n" +
            "  \"name\": \"%s\",\n" + // CONNECTOR_NAME
            "  \"config\": {\n" +
            "    \"connector.class\": \"com.nordstrom.kafka.connect.sqs.SqsSinkConnector\",\n" +
            "    \"tasks.max\": \"1\",\n" +
            "    \"topics\": \"%s\",\n" + // INPUT_TOPIC
            "    \"sqs.region\": \"us-east-1\",\n" +
            "    \"sqs.endpoint.url\": \"%s\",\n" + // localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString()
            "    \"sqs.queue.url\": \"%s\",\n" + // invalidQueueUrl
            "    \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",\n" +
            "    \"value.converter\": \"org.apache.kafka.connect.storage.StringConverter\",\n" +
            "    \"errors.tolerance\": \"all\",\n" +
            "    \"errors.deadletterqueue.topic.name\": \"%s\",\n" + // DLQ_KAFKA_TOPIC
            "    \"errors.deadletterqueue.context.headers.enable\": \"true\"\n" +
            "  }\n" +
            "}",
            CONNECTOR_NAME,
            INPUT_TOPIC,
            localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString(),
            localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString() + "/000000000000/nonExistentQueue" + System.currentTimeMillis(),
            DLQ_KAFKA_TOPIC
        );
        deployConnectorConfig(connectorConfigJson);
        System.out.println("Connector '" + CONNECTOR_NAME + "' deployed with SQS config pointing to invalid queue.");

        // 3. Produce a Message to Kafka
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafka.getBootstrapServers());
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        String testMessageValue = "{\"field\": \"test message that will go to DLQ\"}";
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "testKey", testMessageValue)).get(30, TimeUnit.SECONDS);
            System.out.println("Message sent to " + INPUT_TOPIC + ": " + testMessageValue);
        }

        // 4. Consume from DLQ Kafka Topic
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafka.getBootstrapServers());
        consumerProps.put("group.id", "dlq-consumer-group-" + System.currentTimeMillis()); // Unique group.id
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("auto.offset.reset", "earliest");

        boolean messageFoundInDlq = false;
        long startTime = System.currentTimeMillis();
        ConsumerRecord<String, String> receivedRecord = null;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(DLQ_KAFKA_TOPIC));
            System.out.println("Subscribed to DLQ topic: " + DLQ_KAFKA_TOPIC);

            // Poll for a while to allow message to be processed and sent to DLQ
            while (System.currentTimeMillis() - startTime < 60000 && !messageFoundInDlq) { // 60 seconds timeout
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                if (records.isEmpty()) {
                    System.out.println("Polling DLQ... No records found yet.");
                }
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Found message in DLQ: " + record.value());
                    if (record.value().contains("test message that will go to DLQ")) {
                        messageFoundInDlq = true;
                        receivedRecord = record;
                        // You can also check headers here:
                        // record.headers().forEach(header -> System.out.println("Header: " + header.key() + " = " + new String(header.value())));
                        break;
                    }
                }
            }
        }
        
        Assertions.assertTrue(messageFoundInDlq, "Message not found in DLQ Kafka topic: " + DLQ_KAFKA_TOPIC);
        System.out.println("Message successfully found in DLQ topic.");
        if (receivedRecord != null) {
             System.out.println("DLQ Message Key: " + receivedRecord.key());
             System.out.println("DLQ Message Value: " + receivedRecord.value());
             System.out.println("DLQ Message Partition: " + receivedRecord.partition());
             System.out.println("DLQ Message Offset: " + receivedRecord.offset());
             receivedRecord.headers().forEach(header -> System.out.println("DLQ Header: " + header.key() + " = " + new String(header.value())));
        }
    }
}
