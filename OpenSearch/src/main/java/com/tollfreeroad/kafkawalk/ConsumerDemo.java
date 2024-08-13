package com.tollfreeroad.kafkawalk;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

import java.io.IOException;

public class ConsumerDemo {

    public static void main(String[] args) throws IOException {
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder;
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);

        GetRequest getRequest = new GetRequest("sample-index", "_search");
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);

        System.out.println(response.getSourceAsString());
    }
}
