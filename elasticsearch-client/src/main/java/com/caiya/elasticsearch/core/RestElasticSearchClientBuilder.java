package com.caiya.elasticsearch.core;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Rest es client builder.
 *
 * @author wangnan
 * @since 1.0
 */
public final class RestElasticSearchClientBuilder {

    private static final Logger logger = LoggerFactory.getLogger(RestElasticSearchClientBuilder.class);

    private RestHighLevelClient client;

    private String name;

    private String refresh = "false";

    public static RestElasticSearchClientBuilder create() {
        return new RestElasticSearchClientBuilder();
    }


    public RestElasticSearchClientBuilder clusters(List<String> clusters) {
        return clustersAndSchema(clusters, HttpHost.DEFAULT_SCHEME_NAME);
    }

    public RestElasticSearchClientBuilder clustersAndSchema(List<String> clusters, String schema) {
        HttpHost[] httpHosts = new HttpHost[clusters.size()];
        int index = 0;
        for (String cluster : clusters) {
            httpHosts[index++] = new HttpHost(cluster.split(":")[0], Integer.parseInt(cluster.split(":")[1]), schema);
        }
        client = new RestHighLevelClient(RestClient.builder(httpHosts));
        return this;
    }

    public RestElasticSearchClientBuilder setClientName(String name) {
        this.name = name;
        return this;
    }

    public RestElasticSearchClientBuilder setRefresh(String refresh) {
        this.refresh = refresh == null ? "" : refresh;
        return this;
    }

    public RestElasticSearchClient build() {
        if (client == null) {
            throw new IllegalArgumentException("rest client cannot be null");
        }

        RestElasticSearchClient restElasticSearchClient = new RestElasticSearchClient(client);
        restElasticSearchClient.setName(name);
        restElasticSearchClient.setRefresh(refresh);
        return restElasticSearchClient;
    }


}
