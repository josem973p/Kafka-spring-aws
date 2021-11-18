package com.manuel3p.kafka.configs;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
public class kafkaConfiguration {
	
	private Map<String, Object> consumerProps() { Map<String, Object>props=new HashMap<>();
	props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"b-2.kafka-cluster.xlrzya.c6.kafka.us-east-2.amazonaws.com:9092,b-4.kafka-cluster.xlrzya.c6.kafka.us-east-2.amazonaws.com:9092,b-1.kafka-cluster.xlrzya.c6.kafka.us-east-2.amazonaws.com:9092");
	props.put(ConsumerConfig.GROUP_ID_CONFIG,"group");
	props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
	props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
	return props;

}
	
	
	private Map<String, Object> producerProps() { Map<String, Object> props=new HashMap<>();
	props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"b-2.kafka-cluster.xlrzya.c6.kafka.us-east-2.amazonaws.com:9092,b-4.kafka-cluster.xlrzya.c6.kafka.us-east-2.amazonaws.com:9092,b-1.kafka-cluster.xlrzya.c6.kafka.us-east-2.amazonaws.com:9092");
	props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
	props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
	return props;
	}
	
	@Bean
	public KafkaTemplate<String, String> createTemplate() { Map<String, Object>senderProps= producerProps();
	ProducerFactory<String, String> pf= new
	DefaultKafkaProducerFactory<String, String>(senderProps);
	KafkaTemplate<String, String> template=new KafkaTemplate<>(pf);
	return template;
	}
	
	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
	return new
	DefaultKafkaConsumerFactory<>(consumerProps());
	}
	@Bean
	public ObjectMapper mapper() {
		return new ObjectMapper();
	}
	
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String>kafkaListenerContainerFactory() {
	ConcurrentKafkaListenerContainerFactory<String, String>
	factory = new ConcurrentKafkaListenerContainerFactory<>();
	factory.setConsumerFactory(consumerFactory());
	factory.setConcurrency(50);
	factory.setBatchListener(true);
	return factory;
	}
	
}