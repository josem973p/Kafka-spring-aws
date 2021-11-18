package com.manuel3p.kafka;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;

import models.Dev4jTransaction;

@SpringBootApplication
@EnableScheduling
public class KafkaTransactionsApplication   {
	
	private static final Logger log = LoggerFactory.getLogger(KafkaTransactionsApplication.class);
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	@Autowired
	private ObjectMapper mapper;
	
	
	@Autowired
	private RestHighLevelClient client;
	
	
	

	
	@KafkaListener(topics = "devs4j-transactions",groupId = "devs4jGroup", containerFactory = "kafkaListenerContainerFactory")
	public void listen(List<ConsumerRecord<String, String>> messages) throws JsonMappingException, JsonProcessingException {

		for (ConsumerRecord<String, String> message : messages) {
		//	Dev4jTransaction transaction  = mapper.readValue(message.value(), Dev4jTransaction.class);
		//	log.info("Partition = {} Offset = {}  Key = {} Message = {}",message.partition(),message.offset(),message.key(),message.value());
			IndexRequest indexRequest= buildIndexResquest(String.format("%s-%s-%s", message.partition(),message.key(),message.offset()),message.value());
			client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {

				@Override
				public void onResponse(IndexResponse response) {
					log.debug("Successful request");
					
				}

				@Override
				public void onFailure(Exception e) {
					log.error("Error storing the message {}", e);
					
				}
				
			});
		}
		
		
	
	}
	
	private IndexRequest buildIndexResquest(String key,String value) {
		IndexRequest  request = new IndexRequest("devs4j-transactions");
		request.id(key);
		request.source(value,XContentType.JSON);
		return request;
		
	}

	@Scheduled(fixedRate = 10000)
	public void sendMessages() throws JsonProcessingException {
		Faker faker = new Faker();
		for (int i = 0; i < 100000; i++) {
			Dev4jTransaction transaction = new Dev4jTransaction();
			transaction.setUsername(faker.name().username());
			transaction.setNombre(faker.name().firstName());
			transaction.setApellido(faker.name().lastName());
			transaction.setMonto(faker.number().randomDouble(4, 0, 20000));
			
			kafkaTemplate.send("devs4j-transactions",transaction.getUsername(),mapper.writeValueAsString(transaction));
		}
		
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaTransactionsApplication.class, args);
	}
	
	

}
