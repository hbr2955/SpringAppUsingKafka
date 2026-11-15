package com.tools.kafka.springappusingkafka.service;

import com.tools.kafka.springappusingkafka.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;

import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class KafkaRealTimeNotificationConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaRealTimeNotificationConsumerService.class);

    @KafkaListener(
            topics = "driver-location",
            groupId = "demo-group"
    )
    public void listen(@Payload Message message,
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.OFFSET) long offset){

        logger.info("Message Received from Topic: {}, Partition: {}, Offset: {}", topic, partition, offset);
        logger.info("Message content "+ message );

        try{
            processRealTimeNotification(message);
            logger.info("Message Processed Successfully ");
        }
        catch(Exception e){
            logger.error("Message proceesing failed "+ e.getMessage() );
        }
    }

    private void processRealTimeNotification(Message message) {
        logger.info("Processing Message content "+ message.getMessageContent());

        //ACTUAL BUSINESS LOGIC
        try{
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Processing interrupted" , e);
        }
    }

}
