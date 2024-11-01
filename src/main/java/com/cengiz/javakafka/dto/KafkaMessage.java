package com.cengiz.javakafka.dto;

import java.time.LocalDateTime;
import java.util.UUID;

public class KafkaMessage {

    private String message;

    private String id = UUID.randomUUID().toString();

    private LocalDateTime messageDate = LocalDateTime.now();

    public KafkaMessage(String message) {
        this.message = message;
    }

    public KafkaMessage() {
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public LocalDateTime getMessageDate() {
        return messageDate;
    }

    public void setMessageDate(LocalDateTime messageDate) {
        this.messageDate = messageDate;
    }

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "message='" + message + '\'' +
                ", id='" + id + '\'' +
                ", messageDate=" + messageDate +
                '}';
    }
}