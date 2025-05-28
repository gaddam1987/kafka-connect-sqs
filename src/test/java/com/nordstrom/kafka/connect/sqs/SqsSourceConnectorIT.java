package com.nordstrom.kafka.connect.sqs;

import com.nordstrom.kafka.connect.auth.AWSUserCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.time.Duration;
import java.util.ArrayList;
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

public class SqsSourceConnectorIT {
    private static final Logger log = LoggerFactory.getLogger(SqsSourceConnectorIT.class);

    private static final String TEST_SOURCE_TOPIC = "test-topic-source-it";
    private static final String LOCALSTACK_IMAGE_NAME = "localstack/localstack:1.4.0";
    private static final String CONFLUENT_PLATFORM_VERSION = "7.9.0";

    @Container
    public static LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse(LOCALSTACK_IMAGE_NAME))
            .withServices(LocalStackContainer.Service.SQS)
            .withEnv("DEFAULT_REGION", "us-east-1");

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION));

    private static SqsAsyncClient sqsAsyncClient;
    private String queueUrl;
    private String queueName;

    private SqsSourceConnectorTask task;
    private KafkaConsumer<String, String> consumer;
    private Map<String, String> sourceConnectorConfig;

    @BeforeAll
    public static void setupOnce() {
        if (!localstack.isRunning()) localstack.start();
        if (!kafka.isRunning()) kafka.start();

        sqsAsyncClient = SqsAsyncClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.SQS))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .region(Region.of(localstack.getRegion()))
                .build();
    }

    @AfterAll
    public static void tearDownOnce() {
        if (sqsAsyncClient != null) {
            sqsAsyncClient.close();
        }
        // Testcontainers stop automatically
    }

    @BeforeEach
    public void setUp() throws ExecutionException, InterruptedException {
        queueName = "test-queue-source-" + UUID.randomUUID().toString();
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(queueName).build();
        queueUrl = sqsAsyncClient.createQueue(createQueueRequest).get().queueUrl();
        assertNotNull(queueUrl);
        log.info("Created SQS queue for test: {}", queueUrl);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "source-it-consumer-group-" + UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TEST_SOURCE_TOPIC));

        task = new SqsSourceConnectorTask();
        SourceTaskContext taskContext = Mockito.mock(SourceTaskContext.class);
        OffsetStorageReader offsetStorageReader = Mockito.mock(OffsetStorageReader.class);
        // Configure the mock OffsetStorageReader to return null for any offset query initially.
        // This simulates the task starting with no prior offsets for the given queue.
        Map<String, String> sourcePartition = Collections.singletonMap(SqsConnectorConfigKeys.SQS_QUEUE_URL.getValue(), queueUrl);
        Mockito.when(offsetStorageReader.offset(Mockito.eq(sourcePartition))).thenReturn(null); 
        Mockito.when(taskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        task.initialize(taskContext);

        sourceConnectorConfig = createSourceConnectorConfig(queueUrl, TEST_SOURCE_TOPIC);
    }

    @AfterEach
    public void tearDown() {
        log.info("Tearing down test resources...");
        if (task != null) {
            task.stop();
            log.info("Source task stopped.");
        }
        if (consumer != null) {
            consumer.close();
            log.info("Kafka consumer closed.");
        }
        if (sqsAsyncClient != null && queueUrl != null) {
            try {
                 // Check if queue exists before attempting deletion
                sqsAsyncClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).get();
                sqsAsyncClient.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build()).get();
                log.info("Deleted SQS queue: {}", queueUrl);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof QueueDoesNotExistException) {
                    log.info("SQS queue {} was already deleted or did not exist.", queueUrl);
                } else {
                    log.warn("Error determining if queue {} exists or deleting queue: {}", queueUrl, e.getMessage(), e);
                }
            }
             catch (Exception e) { 
                log.warn("Error during SQS queue cleanup for {}: {}", queueUrl, e.getMessage(), e);
            }
        }
    }

    private Map<String, String> createSourceConnectorConfig(String sqsQueueUrl, String kafkaTopic) {
        Map<String, String> props = new HashMap<>();
        props.put("connector.class", SqsSourceConnector.class.getName());
        props.put("tasks.max", "1");
        props.put(SqsConnectorConfigKeys.TOPICS.getValue(), kafkaTopic); // Kafka topic to produce to
        props.put(SqsConnectorConfigKeys.SQS_QUEUE_URL.getValue(), sqsQueueUrl);
        props.put(SqsConnectorConfigKeys.SQS_REGION.getValue(), localstack.getRegion());
        props.put(SqsConnectorConfigKeys.SQS_ENDPOINT_URL.getValue(), localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString());
        props.put(SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_CLASS_CONFIG.getValue(), AWSUserCredentialsProvider.class.getName());
        props.put(SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_ACCESS_KEY_ID.getValue(), "test");
        props.put(SqsConnectorConfigKeys.CREDENTIALS_PROVIDER_SECRET_ACCESS_KEY.getValue(), "test");
        return props;
    }

    @Test
    public void testSourceMessagesSuccessfully() throws InterruptedException, ExecutionException {
        task.start(sourceConnectorConfig);

        String messageBody1 = "Hello from SQS to Kafka IT! - Message 1 - " + UUID.randomUUID().toString();
        String messageBody2 = "Hello from SQS to Kafka IT! - Message 2 - " + UUID.randomUUID().toString();

        sqsAsyncClient.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody(messageBody1).build()).get();
        sqsAsyncClient.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody(messageBody2).build()).get();
        log.info("Sent 2 messages to SQS queue: {}", queueUrl);

        List<SourceRecord> allPolledRecords = new ArrayList<>();
        int pollAttempts = 0;
        
        while (allPolledRecords.size() < 2 && pollAttempts < 20) { 
            log.info("Polling task, attempt: {}", pollAttempts + 1);
            List<SourceRecord> polled = task.poll(); 
            if (polled != null && !polled.isEmpty()) {
                allPolledRecords.addAll(polled);
                log.info("Task polled {} records. Total received by task so far: {}", polled.size(), allPolledRecords.size());
                for(SourceRecord sr : polled) {
                    task.commitRecord(sr); 
                    log.info("Committed record for SQS message ID: {}", sr.sourceOffset().get(SqsConnectorConfigKeys.SQS_MESSAGE_ID.getValue()));
                }
            } else {
                log.info("Task poll returned no records or null.");
            }
            if (allPolledRecords.size() < 2) { 
                Thread.sleep(1000); 
            }
            pollAttempts++;
        }
        
        assertEquals("Task should have polled 2 records", 2, allPolledRecords.size());

        List<String> receivedKafkaValues = new ArrayList<>();
        int consumeAttempts = 0;
        while (receivedKafkaValues.size() < 2 && consumeAttempts < 10) { 
            log.info("Polling Kafka, attempt: {}. Records collected so far: {}", consumeAttempts + 1, receivedKafkaValues.size());
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(2));
            if (!consumerRecords.isEmpty()) {
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    receivedKafkaValues.add(record.value());
                }
                log.info("Consumer received {} records. Total received from Kafka so far: {}", consumerRecords.count(), receivedKafkaValues.size());
            }
            consumeAttempts++;
        }
        
        assertTrue("Should have consumed message 1 from Kafka", receivedKafkaValues.contains(messageBody1));
        assertTrue("Should have consumed message 2 from Kafka", receivedKafkaValues.contains(messageBody2));
        assertEquals("Should have consumed exactly 2 messages from Kafka", 2, receivedKafkaValues.size());
        log.info("Successfully consumed and verified 2 messages from Kafka topic {}", TEST_SOURCE_TOPIC);

        Thread.sleep(2000); // Give time for SQS deletions to complete
        ReceiveMessageResponse finalSqsState = sqsAsyncClient.receiveMessage(
            ReceiveMessageRequest.builder().queueUrl(queueUrl).maxNumberOfMessages(10).waitTimeSeconds(1).build()
        ).get();
        assertTrue("SQS queue should be empty after successful processing and commit", finalSqsState.messages().isEmpty());
        log.info("SQS queue confirmed empty after processing.");
    }
}
