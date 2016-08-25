package com.test.mmm.kafka.propducer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Marian_Mykhalchuk on 8/17/2016.
 */
@SpringBootApplication
@EnableKafka
public class Application {

	private static final Logger LOGGER = Logger.getLogger(Application.class);
	private static final String TOPIC_NAME = "mmm.demo";

	public static void main(String... s) {
		SpringApplication.run(Application.class, s);
	}

	@Bean
	public CommandLineRunner init(KafkaTemplate<Integer, String> kafkaTemplate) {
		return (args) -> {
			ListenableFuture<SendResult<Integer, String>> listener = kafkaTemplate.send(TOPIC_NAME, Integer.valueOf
					(4234230), "spring-boot-kafka message");
			listener.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
				@Override
				public void onFailure(Throwable throwable) {
					LOGGER.error("<<<<< Send message failed >>>>>>", throwable);
				}

				@Override
				public void onSuccess(SendResult<Integer, String> integerStringSendResult) {
					LOGGER.info(String.format("IT WORKS WITH NEXT " +
									"DATA:\ntopic\t->\t%s\noffset\t->\t%s\npartition\t->\t%s",
							integerStringSendResult.getRecordMetadata().topic(),
							integerStringSendResult.getRecordMetadata().offset(),
							integerStringSendResult.getRecordMetadata().partition()));
				}
			});
		};
	}

	@Bean
	public ProducerFactory<Integer, String> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	@Bean
	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return props;
	}

	@Bean
	public KafkaTemplate<Integer, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

}
