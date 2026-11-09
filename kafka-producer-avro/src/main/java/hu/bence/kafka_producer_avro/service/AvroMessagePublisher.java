package hu.bence.kafka_producer_avro.service;

import hu.bence.kafka_producer_avro.KafkaProducerApplication;
import java.util.UUID;
import java.util.Random;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Qualifier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class AvroMessagePublisher {

    private final KafkaProducerApplication kafkaProducerApplication;

    private static final String TOPIC = "avro-producer-test-topic";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Schema schema;

    public AvroMessagePublisher(
        @Qualifier("avroKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate,
        KafkaProducerApplication kafkaProducerApplication
    ) throws IOException {
        this.kafkaTemplate = kafkaTemplate;
        try (var schemaFile = new ClassPathResource("avro/AvroModel.avsc").getInputStream()) {
            this.schema = new Schema.Parser().parse(schemaFile);
        }
        this.kafkaProducerApplication = kafkaProducerApplication;
    }

    public CompletableFuture<SendResult<String, Object>> publishSample() {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", new Random().nextInt());
        record.put("name", UUID.randomUUID().toString());
        return kafkaTemplate.send(TOPIC, record);
    }
}
