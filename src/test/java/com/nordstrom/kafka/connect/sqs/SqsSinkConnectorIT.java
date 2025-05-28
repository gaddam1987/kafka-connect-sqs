package com.nordstrom.kafka.connect.sqs;

import com.nordstrom.kafka.connect.auth.AWSUserCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SqsSinkConnectorIT extends SQSContainerTest {
    private static final Logger log = LoggerFactory.getLogger(SqsSinkConnectorIT.class);
    private static final String TEST_SINK_TOPIC = "sink-test-topic";
    private static final String TEST_DLQ_TOPIC = "sink-test-dlq-topic";


    private static SqsAsyncClient sqsAsyncClient;
    private String queueUrl;
    private String queueName;
    private String dlqQueueUrl; // For tests that might use an SQS DLQ, not Kafka DLQ
    private String dlqQueueName;


    private SqsSinkConnectorTask task;
    private KafkaProducer<String, String> producer;
    private Map<String, String> sinkConnectorConfig;


    @BeforeAll
    public static void setupOnce() {
        SQSContainerTest.setup();

        sqsAsyncClient = SqsAsyncClient.builder()
                .endpointOverride(SQSContainerTest.localstackContainer.getEndpointOverride(LocalStackContainer.Service.SQS))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .region(Region.of(SQSContainerTest.localstackContainer.getRegion()))
                .build();

        String.format("http://%s:%d", SQSContainerTest.kafkaConnectContainer.getHost(), SQSContainerTest.kafkaConnectContainer.getMappedPort(8083));

    }

    @AfterAll
    public static void tearDownOnce() {
        if (sqsAsyncClient != null) {
            sqsAsyncClient.close();
        }
    }


    @BeforeEach
    public void setUp() throws ExecutionException, InterruptedException {
        queueName = "test-queue-sink-" + UUID.randomUUID().toString();
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(queueName).build();
        queueUrl = sqsAsyncClient.createQueue(createQueueRequest).get().queueUrl();
        Assertions.assertNotNull(queueUrl);
        log.info("Created SQS queue for test: {}", queueUrl);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SQSContainerTest.kafkaContainer.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        task = new SqsSinkConnectorTask();
        SinkTaskContext taskContext = Mockito.mock(SinkTaskContext.class); 
        task.initialize(taskContext);

        // Default config, can be overridden in tests
        sinkConnectorConfig = createBaseSinkConnectorConfig(queueUrl, TEST_SINK_TOPIC);
    }

    @AfterEach
    public void tearDown() {
        log.info("Tearing down test resources...");
        if (task != null) {
            task.stop();
            log.info("Sink task stopped.");
        }
        if (producer != null) {
            producer.close();
            log.info("Kafka producer closed.");
        }
        deleteTestQueue(queueUrl, queueName);
        if (dlqQueueUrl != null) { // If a test created a DLQ SQS queue
            deleteTestQueue(dlqQueueUrl, dlqQueueName);
            dlqQueueUrl = null;
            dlqQueueName = null;
        }
    }
    
    private void deleteTestQueue(String qUrl, String qName) {
        if (sqsAsyncClient != null && qUrl != null) {
            try {
                // Check if queue exists before attempting deletion
                sqsAsyncClient.getQueueUrl(req -> req.queueName(qName)).get(); 
                sqsAsyncClient.deleteQueue(DeleteQueueRequest.builder().queueUrl(qUrl).build()).get();
                log.info("Deleted SQS queue: {}", qUrl);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof QueueDoesNotExistException) {
                    log.info("SQS queue {} was already deleted or does not exist.", qUrl);
                } else {
                    log.warn("Error deleting queue {}: {}", qUrl, e.getMessage(), e);
                }
            }
            catch (Exception e) { 
                log.warn("Error during queue deletion check for {}: {}", qUrl, e.getMessage(), e);
            }
        }
    }


    private Map<String, String> createBaseSinkConnectorConfig(String sqsQueueUrl, String kafkaTopics) {
        Map<String, String> props = new HashMap<>();
        props.put("connector.class", SqsSinkConnector.class.getName());
        props.put("tasks.max", "1"); // Replace with the correct key for tasks.max
        props.put(SqsConnectorConfigKeys.TOPICS.getValue(), kafkaTopics);
        props.put(SqsConnectorConfigKeys.SQS_QUEUE_URL.getValue(), sqsQueueUrl);
        props.put(SqsConnectorConfigKeys.SQS_REGION.getValue(), SQSContainerTest.localstackContainer.getRegion());
        props.put(SqsConnectorConfigKeys.SQS_ENDPOINT_URL.getValue(), SQSContainerTest.localstackContainer.getEndpointOverride(LocalStackContainer.Service.SQS).toString());
        props.put(SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_CLASS_CONFIG.getValue(), AWSUserCredentialsProvider.class.getName());
        props.put(SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_ACCESS_KEY_ID.getValue(), "test");
        props.put(SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_SECRET_ACCESS_KEY.getValue(), "test");
        // Required for DLQ Producer in SqsSinkConnectorTask start method
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SQSContainerTest.kafkaContainer.getBootstrapServers());
        return props;
    }

    @Test
    public void testSinkMessageSuccessfully() throws InterruptedException, ExecutionException {
        task.start(sinkConnectorConfig);

        String testMessageKey = "key-" + UUID.randomUUID().toString();
        String testMessageValue = "Hello SQS from Kafka Connect IT! - " + UUID.randomUUID().toString();

        producer.send(new ProducerRecord<>(TEST_SINK_TOPIC, 0, testMessageKey, testMessageValue)).get();
        producer.flush();
        log.info("Produced message to Kafka: key={}, value={}", testMessageKey, testMessageValue);

        SinkRecord sinkRecord = new SinkRecord(
                TEST_SINK_TOPIC, 0, Schema.STRING_SCHEMA, testMessageKey, Schema.STRING_SCHEMA, testMessageValue,
                0, System.currentTimeMillis(),TimestampType.CREATE_TIME, new ConnectHeaders()
        );
        task.put(Collections.singletonList(sinkRecord));
        log.info("Called task.put() with one record.");

        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl).maxNumberOfMessages(1).waitTimeSeconds(10).build();
        List<Message> messages = Collections.emptyList();
        for (int attempts = 0; messages.isEmpty() && attempts < 5; attempts++) {
            log.info("Attempt {} to receive message from SQS queue: {}", attempts + 1, queueUrl);
            messages = sqsAsyncClient.receiveMessage(receiveRequest).get().messages();
            if (messages.isEmpty()) Thread.sleep(2000);
        }
        
        assertTrue("Should have received at least 1 message from SQS", messages.size() >= 1);
        Message sqsMessage = messages.get(0);
        assertEquals("Message body should match", testMessageValue, sqsMessage.body());
        log.info("Successfully received and verified message from SQS: {}", sqsMessage.body());

        sqsAsyncClient.deleteMessage(DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(sqsMessage.receiptHandle()).build()).get();
        log.info("Deleted message from SQS.");
    }

    @Test
    public void testSinkToDlqOnSqsFailure() throws InterruptedException, ExecutionException {
        // Configure DLQ
        sinkConnectorConfig.put("errors.deadletterqueue.topic.name", TEST_DLQ_TOPIC);
        sinkConnectorConfig.put("errors.tolerance", "all");
        sinkConnectorConfig.put("errors.deadletterqueue.context.headers.enable", "true");
        task.start(sinkConnectorConfig);

        // Simulate SQS failure by deleting the queue before task.put()
        final String originalQueueUrl = this.queueUrl; // Save for DLQ verification
        sqsAsyncClient.deleteQueue(DeleteQueueRequest.builder().queueUrl(originalQueueUrl).build()).get();
        log.info("Deleted SQS queue {} to simulate send failure.", originalQueueUrl);
        // Give a moment for queue deletion to propagate if necessary
        Thread.sleep(1000);


        String testMessageKey = "dlq-key-" + UUID.randomUUID().toString();
        String testMessageValue = "Message destined for DLQ - " + UUID.randomUUID().toString();
        
        SinkRecord sinkRecord = new SinkRecord(
                TEST_SINK_TOPIC, 0, Schema.STRING_SCHEMA, testMessageKey, Schema.STRING_SCHEMA, testMessageValue,
                1, System.currentTimeMillis(), TimestampType.CREATE_TIME, new ConnectHeaders()
        );
        task.put(Collections.singletonList(sinkRecord));
        log.info("Called task.put() with one record expected to go to DLQ.");

        // Setup Kafka consumer for DLQ topic
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SQSContainerTest.kafkaContainer.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-consumer-group-" + UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<byte[], byte[]> dlqConsumer = null;
        try {
            dlqConsumer = new KafkaConsumer<>(consumerProps);
            dlqConsumer.subscribe(Collections.singletonList(TEST_DLQ_TOPIC));
            
            ConsumerRecords<byte[], byte[]> dlqRecords = ConsumerRecords.empty();
            int attempts = 0;
            while(dlqRecords.isEmpty() && attempts < 10) { // Increased attempts/timeout for DLQ
                log.info("Attempt {} to receive message from DLQ topic: {}", attempts + 1, TEST_DLQ_TOPIC);
                dlqRecords = dlqConsumer.poll(Duration.ofSeconds(2));
                if (dlqRecords.isEmpty()) Thread.sleep(1000);
                attempts++;
            }

            assertTrue("Should have received at least 1 message on DLQ topic", !dlqRecords.isEmpty());
            
            ConsumerRecord<byte[], byte[]> dlqMessage = dlqRecords.iterator().next();
            assertNotNull("DLQ message should not be null", dlqMessage);
            assertEquals("DLQ message value should match original", testMessageValue, new String(dlqMessage.value(), StandardCharsets.UTF_8));
            log.info("Successfully received message from DLQ: {}", new String(dlqMessage.value(), StandardCharsets.UTF_8));

            boolean exceptionClassHeaderFound = false;
            boolean exceptionMessageHeaderFound = false;
            for (org.apache.kafka.common.header.Header header : dlqMessage.headers()) {
                if (header.key().equals("x-connect-sqs-dlq-exception-class")) {
                    exceptionClassHeaderFound = true;
                    String exceptionClass = new String(header.value(), StandardCharsets.UTF_8);
                    log.info("DLQ Exception Class Header: {}", exceptionClass);
                    assertTrue("Exception class should indicate SdkException or a subclass", exceptionClass.contains("software.amazon.awssdk.core.exception.SdkException") || exceptionClass.contains("QueueDoesNotExistException"));
                }
                if (header.key().equals("x-connect-sqs-dlq-exception-message")) {
                    exceptionMessageHeaderFound = true;
                    log.info("DLQ Exception Message Header: {}", new String(header.value(), StandardCharsets.UTF_8));
                }
            }
            assertTrue("DLQ should have exception class header", exceptionClassHeaderFound);
            assertTrue("DLQ should have exception message header", exceptionMessageHeaderFound);

        } finally {
            if (dlqConsumer != null) {
                dlqConsumer.close();
            }
        }

        // Verify message is NOT in the (now deleted) SQS queue
        try {
            ReceiveMessageResponse receiveResponse = sqsAsyncClient.receiveMessage(
                ReceiveMessageRequest.builder().queueUrl(originalQueueUrl).maxNumberOfMessages(1).waitTimeSeconds(1).build()
            ).get();
            assertTrue("SQS queue should be empty or non-existent", receiveResponse.messages().isEmpty());
        } catch (ExecutionException e) {
            assertTrue("Exception when receiving from deleted queue should be QueueDoesNotExistException or similar SdkException", 
                       e.getCause() instanceof QueueDoesNotExistException || e.getCause() instanceof SdkException);
            log.info("Correctly received exception when trying to read from deleted queue: {}", e.getCause().getMessage());
        }
    }
}
