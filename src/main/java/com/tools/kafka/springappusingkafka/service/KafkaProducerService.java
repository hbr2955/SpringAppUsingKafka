package com.tools.kafka.springappusingkafka.service;

import com.tools.kafka.springappusingkafka.model.Message;
import org.springframework.kafka.support.SendResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    private static final String TOPIC = "driver-location";

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void sendMessage(Message message){
        logger.info("Sending message to the TOPIC " + TOPIC);
        CompletableFuture<SendResult<String,Object>> future = kafkaTemplate.send(TOPIC, message.getId(), message);
        future.whenComplete((result,exception)->{
            if(exception!=null){
                logger.error("Failed to send Message",exception.getMessage());
            }
            else{
                logger.info("Message Sent with the Offset: " + result.getRecordMetadata().offset());
            }
        });
    }

    public void sendMessage(String messageId, String messageContent, String sender) {
        Message message = new Message(messageId,messageContent,sender);
        sendMessage(message);
    }
}
