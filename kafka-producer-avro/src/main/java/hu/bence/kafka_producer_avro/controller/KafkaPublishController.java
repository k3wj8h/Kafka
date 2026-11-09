package hu.bence.kafka_producer_avro.controller;

import hu.bence.kafka_producer_avro.service.AvroMessagePublisher;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.*;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/publish")
public class KafkaPublishController {

    private final AvroMessagePublisher avroPublisher;

    public KafkaPublishController(
        AvroMessagePublisher avroPublisher
    ) {
        this.avroPublisher = avroPublisher;
    }


    @GetMapping("/sample")
    public CompletableFuture<ResponseEntity<String>> publishAvroSample() {
        return avroPublisher.publishSample()
                            .thenApply(result -> ResponseEntity.accepted().body("Avro message published at offset " + result.getRecordMetadata().offset()));
    }

}