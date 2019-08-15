package com.caiya.elasticsearch.core;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * ElasticSearch client builder.
 *
 * @author wangnan
 * @since 1.0
 */
public final class ElasticSearchClientBuilder {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClientBuilder.class);

    private TransportClient client;

    private String refreshPolicy = "false";

    private boolean refresh = false;

    private ElasticSearchClientBuilder() {
    }

    public static ElasticSearchClientBuilder create(Map<String, String> settings, List<String> clusters) {
        return new ElasticSearchClientBuilder()
                .settingsAndClusters(settings, clusters);
    }

    private ElasticSearchClientBuilder settingsAndClusters(Map<String, String> settings, List<String> clusters) {
        return settingsAndClusters(settings, clusters, settings.containsKey(ElasticSearchConstant.XPACK_AUTH_SETTING));
    }

    private ElasticSearchClientBuilder settingsAndClusters(Map<String, String> settingsMap, List<String> clusters, Boolean withXPack) {
        if (CollectionUtils.isEmpty(clusters))
            throw new IllegalArgumentException("cluster nodes list is empty");

        Settings.Builder builder = Settings.builder();
        if (MapUtils.isNotEmpty(settingsMap)) {
            builder.put(Settings.builder().putProperties(settingsMap, s -> s).build());
        }
        Settings settings = builder.build();
        try {
            if (Objects.equals(true, withXPack)) {
                client = new PreBuiltXPackTransportClient(settings);
            } else {
                client = new PreBuiltTransportClient(settings);
            }
            for (String transportAddress : clusters) {
                String host = transportAddress.split(":")[0];
                int port = Integer.parseInt(transportAddress.split(":")[1]);
                client.addTransportAddress(new TransportAddress(InetAddress.getByName(host), port));
            }
        } catch (Exception e) {
            logger.error("elasticsearch client init failed", e);
            if (client != null) {
                try {
                    client.close();
                    client = null;// help gc
                } catch (Exception e1) {
                    logger.error("elasticsearch client close failed", e);
                }
            }
        }
        return this;
    }

    public ElasticSearchClientBuilder setRefreshPolicy(String refreshPolicy) {
        this.refreshPolicy = refreshPolicy == null ? "" : refreshPolicy;
        return this;
    }

    public ElasticSearchClientBuilder setRefresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public ElasticSearchClient build() {
        return buildWithClientName(null);
    }

    public ElasticSearchClient buildWithClientName(String name) {
        if (client == null) {
            throw new IllegalArgumentException("transport client cannot be null");
        }

        ElasticSearchClient elasticSearchClient = new ElasticSearchClient(client);
        elasticSearchClient.setName(name);
        elasticSearchClient.setRefreshPolicy(refreshPolicy);
        elasticSearchClient.setRefresh(refresh);
        return elasticSearchClient;
    }

}
