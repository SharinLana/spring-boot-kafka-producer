package org.example.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.kafkademo.dto.CreateProductDto;
import org.example.kafkademo.service.ProductService;
import org.example.kafkademocore.ProductCreatedEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@DirtiesContext // resets Spring context after test to avoid side effects.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
// uses one test class instance for all tests.
@ActiveProfiles("test") // activates application-test.properties for testing
@EmbeddedKafka(partitions = 3, count = 3, controlledShutdown = true) // count ==
// number of brokers
@SpringBootTest(properties = "spring.kafka.producer" +
                             ".bootstrap-servers=${spring.embedded.kafka" +
                             ".brokers}") // loads full Spring Boot context
// with given properties ( a must-have annotation, required for all the
// components to interact together in the test)
// spring.embedded.kafka.brokers auto-created by Spring Kafka’s test support
// when you use @EmbeddedKafka
public class ProductsServiceIntegrationTest {

    @Autowired
    private ProductService productService;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private Environment environment;

    private KafkaMessageListenerContainer<String, ProductCreatedEvent> container;
    private BlockingQueue<ConsumerRecord<String, ProductCreatedEvent>> records;

    @BeforeAll
    void setUp() {
        // Create a Kafka consumer factory that will generate Kafka consumers
        // using the consumer properties we defined in getConsumerProperties()
        DefaultKafkaConsumerFactory<String, Object> consumerFactory =
                new DefaultKafkaConsumerFactory<>(getConsumerProperties());

        // Define which topic(s) this test container should listen to.
        // The topic name is read from application-test.properties.
        ContainerProperties containerProperties =
                new ContainerProperties(environment.getProperty("product-created-events-topic-name"));

        // Create a KafkaMessageListenerContainer that binds the consumer factory
        // with the container properties (topic subscription info).
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

        // Create an in-memory queue to temporarily store all consumed Kafka records
        // so that test methods can retrieve them for assertions.
        records = new LinkedBlockingQueue<>();

        // Register a listener with the container that will add each consumed
        // ProductCreatedEvent into the queue as soon as it is received.
        container.setupMessageListener((MessageListener<String, ProductCreatedEvent>) records::add);

        // Start the container so it begins consuming messages from Kafka.
        container.start();

        // Block until the container has been assigned partitions from the embedded broker,
        // ensuring it is ready before the test starts producing messages.
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
    }


    @Test // without it JUnit would not run this method
    // naming convention: what is tested_under what conditions_expected output
    void testCreateProduct_whenGivenValidProductDetails_successfulSendKafkaMessage() throws InterruptedException {

        // Arrange: initialize a dto object and prepare data that will be
        // used in the test
        // the createProduct() method receives a dto that we need to mock
        String title = "iPhone 11";
        BigDecimal price = new BigDecimal("650");
        Integer quantity = 1;

        CreateProductDto product = new  CreateProductDto();
        product.setTitle(title);
        product.setPrice(price);
        product.setQuantity(quantity);

        // Act
        productService.createProduct(product); // NOTE: if the method is
        // synchronous and throws an exception, then throw the Exception for
        // the test method here as well


        // Assert.
        // To verify that the producer successfully sent a message, we need to
        // check if there's a new message in a kafkaTopic.
        // So I need to create a kafkaConsumer, consume a message and then
        // check id this message matches the message I just sent.

        // 1. Config a kafka consumer as a private method (see below)
        // 2.Get the message from the records queue
        ConsumerRecord<String, ProductCreatedEvent> message =
                records.poll(3000, TimeUnit.MILLISECONDS); // waits up to 3
        // seconds for a message, return it if arrives, if not - return null

        assertNotNull(message);
        assertNotNull(message.key());

        ProductCreatedEvent event = message.value(); // basically,ProductCreatedEvent
        assertEquals(product.getQuantity(), event.getQuantity());
        assertEquals(product.getTitle(), event.getTitle());
        assertEquals(product.getPrice(), event.getPrice());

    }

    private Map<String, Object> getConsumerProperties() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class,
                ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("spring.kafka.consumer.group-id"),
                JsonDeserializer.TRUSTED_PACKAGES, environment.getProperty(
                        "spring.kafka.consumer.properties.spring.json.trusted.packages"),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, environment.getProperty(
                        "spring.kafka.consumer.auto-offset-reset")
        );
    }


    @AfterAll
    void tearDown() {
        container.stop();
    }

}
