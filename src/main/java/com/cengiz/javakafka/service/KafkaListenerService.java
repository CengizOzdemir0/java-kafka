package com.cengiz.javakafka.service;

import com.cengiz.javakafka.dto.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    @KafkaListener(
            topicPartitions = @TopicPartition(
                    topic = "${fd.kafka.topic}",
                    partitionOffsets = { @PartitionOffset(
                            partition = "0",
                            initialOffset = "0") }),
            groupId = "${fd.kafka.group.id}"
    )
    public void listen(@Payload KafkaMessage kafkaMessage) {
        logger.info("Received Message: " +
                "MessageInfo : {}", kafkaMessage);
    }
}