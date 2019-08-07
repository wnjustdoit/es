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

    private String refreshPolicy = "false";

    private boolean refresh = false;

    public static RestElasticSearchClientBuilder create(List<String> clusters) {
        return create(clusters, HttpHost.DEFAULT_SCHEME_NAME);
    }

    public static RestElasticSearchClientBuilder create(List<String> clusters, String schema) {
        return new RestElasticSearchClientBuilder()
                .clustersAndSchema(clusters, schema);
    }

    private RestElasticSearchClientBuilder clustersAndSchema(List<String> clusters, String schema) {
        try {
            HttpHost[] httpHosts = new HttpHost[clusters.size()];
            int index = 0;
            for (String cluster : clusters) {
                httpHosts[index++] = new HttpHost(cluster.split(":")[0], Integer.parseInt(cluster.split(":")[1]), schema);
            }
            this.client = new RestHighLevelClient(RestClient.builder(httpHosts));
        } catch (Exception e) {
            logger.error("elasticsearch rest client init failed", e);
            if (client != null) {
                try {
                    client.close();
                    client = null;// help gc
                } catch (Exception e1) {
                    logger.error("elasticsearch rest client close failed", e);
                }
            }
        }
        return this;
    }

    public RestElasticSearchClientBuilder setRefreshPolicy(String refreshPolicy) {
        this.refreshPolicy = (refreshPolicy == null) ? "" : refreshPolicy;
        return this;
    }

    public RestElasticSearchClientBuilder setRefresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public RestElasticSearchClient build() {
        return buildWithClientName(null);
    }

    public RestElasticSearchClient buildWithClientName(String name) {
        if (client == null) {
            logger.error("rest client cannot be null");
            throw new IllegalArgumentException("rest client cannot be null");
        }

        RestElasticSearchClient restElasticSearchClient = new RestElasticSearchClient(client);
        restElasticSearchClient.setName(name);
        restElasticSearchClient.setRefreshPolicy(refreshPolicy);
        restElasticSearchClient.setRefresh(refresh);
        return restElasticSearchClient;
    }

    /**
     * @return elasticsearch low-level client
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low-usage-requests.html"/>low-level client usage</a>
     */
    public RestClient buildOriginalLowLevelClient() {
        if (client == null) {
            logger.error("rest client cannot be null");
            throw new IllegalArgumentException("rest client cannot be null");
        }

        return client.getLowLevelClient();
    }


}
