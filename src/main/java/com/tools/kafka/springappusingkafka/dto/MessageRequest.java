package com.tools.kafka.springappusingkafka.dto;

import lombok.Data;

@Data
public class MessageRequest {
    private String messageContent;
    private String sender;
}
