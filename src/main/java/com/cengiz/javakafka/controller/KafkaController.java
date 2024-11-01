package com.cengiz.javakafka.controller;

import com.cengiz.javakafka.dto.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);


    @Value("${fd.kafka.topic}")
    private String kafkaTopic;

    @Autowired
    private KafkaTemplate<String, KafkaMessage> kafkaTemplate;

    @PostMapping
    public void sendMessage(@RequestBody KafkaMessage kafkaMessage) {
        // UUID ile mesaj için bir anahtar oluştur
        String messageKey = UUID.randomUUID().toString();

        // Mesajı Kafka'ya gönder
        ListenableFuture<SendResult<String, KafkaMessage>> future = (ListenableFuture<SendResult<String, KafkaMessage>>) kafkaTemplate.send(kafkaTopic, messageKey, kafkaMessage);

        // Başarı ve hata durumları için geri dönüş (callback) işlemleri
        future.addCallback(new ListenableFutureCallback<SendResult<String, KafkaMessage>>() {
            @Override
            public void onSuccess(SendResult<String, KafkaMessage> result) {
                logger.info("Sent message=[{}] with offset=[{}]", kafkaMessage.getMessage(), result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                logger.error("Unable to send message=[{}] due to: {}", kafkaMessage.getMessage(), ex.getMessage());
            }
        });
    }

}