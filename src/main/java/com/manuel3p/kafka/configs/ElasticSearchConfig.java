package com.manuel3p.kafka.configs;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticSearchConfig {
	
	@Bean(destroyMethod = "close")
	public RestHighLevelClient  createClient() {
				
				final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
				credentialsProvider.setCredentials(AuthScope.ANY,new
						UsernamePasswordCredentials("manuel3p","gswcJmcc97@3p"));
				
				RestHighLevelClient client=new RestHighLevelClient(
				RestClient.builder(new HttpHost("search-manuel3p-cq2bko66cyve6wmglfvgwxw354.us-east-2.es.amazonaws.com", 443,"https"))
				.setHttpClientConfigCallback((config)-> config.setDefaultCredentialsProvider(credentialsProvider)));
				
				return client;
	}

}
