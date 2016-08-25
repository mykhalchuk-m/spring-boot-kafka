package com.test.mmm.kafka.consumer.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Created by Marian_Mykhalchuk on 8/18/2016.
 */
@Component
public class TopicListener {

	private static final Logger LOGGER = Logger.getLogger(TopicListener.class);
	private static final String TOPIC_NAME = "mmm.demo";

	private final SimpMessagingTemplate template;

	@Autowired
	public TopicListener(SimpMessagingTemplate template) {
		this.template = template;
	}

	@KafkaListener(id = "mmm.demo.listener", topics = {TOPIC_NAME})
	public void listen(ConsumerRecord<Integer, String> consumerRecord) throws IOException {
		LOGGER.info(String.format("Consumed message:\nkey\t->\t%s\nvalue\t->\t%s\n" +
						"topic\t->\t%s\noffset\t->\t%s\npartition\t->\t%s",
				consumerRecord.key(), consumerRecord.value(), consumerRecord.topic(),
				consumerRecord.offset(), consumerRecord.partition()));
		template.convertAndSend("/topic/data", consumerRecord.value());
	}
}
