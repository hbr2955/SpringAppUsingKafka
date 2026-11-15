package com.tools.kafka.springappusingkafka.controller;


import com.tools.kafka.springappusingkafka.dto.MessageRequest;
import com.tools.kafka.springappusingkafka.service.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/messages")
public class MessageController {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @GetMapping("/checkhealth")
    public ResponseEntity<String> checkHealth() {
        return ResponseEntity.ok("Kafka Server is running");
    }

    @PostMapping("/sendmessage")
    public ResponseEntity<String> sendMessage(@RequestBody MessageRequest messageRequest) {
        String messageId = UUID.randomUUID().toString();
        kafkaProducerService.sendMessage(messageId,messageRequest.getMessageContent(),messageRequest.getSender());
        return ResponseEntity.ok("Message Sent With the ID: " + messageId);
    }


}
