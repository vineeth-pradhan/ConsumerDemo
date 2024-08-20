package com.tollfreeroad.kafkawalk;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    private static KafkaConsumer<String, String> getKafkaClient() {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "generic");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    private static RestHighLevelClient getOsClient(){
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder;
                    }
                });
        RestHighLevelClient osClient = new RestHighLevelClient(builder);
        return osClient;
    }

    public static void main(String[] args) throws IOException {
        RestHighLevelClient osClient = getOsClient();

        try(osClient) {
            boolean indexExists = osClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if(!indexExists){
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                osClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Wikimedia index is created");
            } else {
                log.info("Wikimedia index already exists");
            }

            KafkaConsumer<String, String> consumerClient = getKafkaClient();

            consumerClient.subscribe(Arrays.asList("wikimedia.recentchange"));
            while (true) {
                ConsumerRecords<String, String> records = consumerClient.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, String> record : records){
                    try {
                        IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON);
                        IndexResponse response = osClient.index(indexRequest, RequestOptions.DEFAULT);
                        log.info(response.getId());
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
//                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                }
            }
        }
    }
}
