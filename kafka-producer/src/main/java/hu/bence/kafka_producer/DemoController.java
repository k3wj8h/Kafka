package hu.bence.kafka_producer;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
public class DemoController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC = "kafka-producer-test-topic";

    @GetMapping("/publish/{message}")
    public String publishMessage(@PathVariable("message") String message) {

        kafkaTemplate.send(TOPIC, message);

        return "Message Published Successfully";
    }
}