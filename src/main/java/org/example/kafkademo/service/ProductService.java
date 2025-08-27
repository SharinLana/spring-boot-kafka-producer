package org.example.kafkademo.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.example.kafkademo.service.interfaces.ProductServiceInterface;
import org.example.kafkademo.dto.CreateProductDto;
import org.example.kafkademocore.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
public class ProductService implements ProductServiceInterface {
    // <String, ProductCreatedEvent> - specifying a Kafka message key-value pair
    private final KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    // initialize KafkaTemplate variable using constructor
    public ProductService(
            KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // ASYNC WAY of handling createProduct()
    @Override
    public String createProduct(
            CreateProductDto dto) {
        // 1. generate a string id for the topic
        String productId = UUID.randomUUID().toString();

        // TODO: persist Product details into DB before publishing event

        // 2. create a product event
        ProductCreatedEvent productCreateEvent =
                new ProductCreatedEvent(productId, dto.getTitle(),
                                        dto.getPrice(), dto.getQuantity());

        // 3. to move the event to Kafka topic, we can use a special client
        // provided by springframework - KafkaTemplate. It's a small wrapper
        // for a KafkaProducer that has such features as dependency injection
        // and automatic configuration (see above, line 13 - 19)

        // Generate a record with the unique ids (one - for the record
        // itself, another - for the message/event)
        ProducerRecord<String, ProductCreatedEvent> record =
                new ProducerRecord<>("product-created-events-topic",// taken from
                                     // KafkaConfig
                                     productId, productCreateEvent);
        record.headers().add("messageId",
                             UUID.randomUUID().toString().getBytes()); //
        // generated an id for the message and stored
        // it into headers to prevent storing duplicate
        // messages into the database in the Consumer microservice

        // KafkaTemplate provides us with a number of send() methods that can
        // be used to send messages to Kafka topic:
        // + Get a confirmation that product event successfully persist in
        // Kafka topic (we are sending a message asynchronously but still
        // getting a notification about completion the operation thanks to
        // the CompletableFuture Java class)
        CompletableFuture<SendResult<String, ProductCreatedEvent>> future =
                kafkaTemplate.send(record);

        // 4. to handle the result when the operation completes, we can use a
        // whenComplete() method:
        future.whenComplete((r, e) -> {
            if (e != null) {
                // log the error message using LOGGER
                LOGGER.error("Failed to send message: " + e.getMessage());
            } else {
                LOGGER.info("Message sent to topic: " + r.getRecordMetadata());
                // logging metadata:
                RecordMetadata metadata = r.getRecordMetadata();
                LOGGER.info("Sent to topic {}, partition {}, offset {}",
                            metadata.topic(), metadata.partition(),
                            metadata.offset());
            }
        });

        // 5. (optional) the method below will block the current thread until
        // the future is
        // complete and return the result of computation when it's available.
        // So, basically it will make the entire async  operation synchronous.
        // If you DO NOT want to wait for a response from the whenComplete
        // method, then you cen remove the following line:

        //        future.join();


        // 6.
        return productId;
    }

    // ======== SYNC WAY of handling createProduct() (waiting for the
    // acknowledgement from all
    // kafka
    // brokers that the message successfully stored in kafka topic ========
    /*
    @Override
    public String createProduct(
            CreateProductDto dto) throws ExecutionException,
            InterruptedException {
        // 1. generate a string id for the topic
        String productId = UUID.randomUUID().toString();

        // TODO: persist Product details into DB before publishing event

        // 2. create a product event
        ProductCreatedEvent productCreateEvent =
                new ProductCreatedEvent(productId, dto.getTitle(),
                                        dto.getPrice(), dto.getQuantity());

        // 3. to move the event to Kafka topic, we can use a special client
        // provided by springframework - KafkaTemplate. It's a small wrapper
        // for a KafkaProducer that has such features as dependency injection
        // and automatic configuration (see above, line 13 - 19)

        // KafkaTemplate provides us with a number of send() methods that can
        // be used to send messages to Kafka topic:
        // + Get a confirmation that product event successfully persist in
        // Kafka topic
        SendResult<String, ProductCreatedEvent> result =
                kafkaTemplate.send("product-created-events-topic",// taken from
                                   // KafkaConfig
                                   productId, productCreateEvent).get(); //
        // if using a sync way, we need to call a get() method right after
        // send() + throw exceptions

        // logging metadata:
                RecordMetadata metadata = result.getRecordMetadata();
                LOGGER.info("Sent to topic {}, partition {}, offset {}",
                            metadata.topic(), metadata.partition(),
                            metadata.offset());

        return productId;
    }

     */
}

