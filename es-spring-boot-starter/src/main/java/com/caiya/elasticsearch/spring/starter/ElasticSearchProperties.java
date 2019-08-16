package com.caiya.elasticsearch.spring.starter;

import com.caiya.elasticsearch.EsClient;
import org.elasticsearch.action.support.WriteRequest;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "es")
public class ElasticSearchProperties {

    private boolean enabled = true;

    private Map<String, String> settings;

    private List<String> clusters = Collections.singletonList("localhost:9200");

    private EsClient.Type type = EsClient.Type.REST_HIGH_LEVEL;

    private WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.NONE;

    private boolean refresh = false;


    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Map<String, String> getSettings() {
        return settings;
    }

    public void setSettings(Map<String, String> settings) {
        this.settings = settings;
    }

    public List<String> getClusters() {
        return clusters;
    }

    public void setClusters(List<String> clusters) {
        this.clusters = clusters;
    }

    public EsClient.Type getType() {
        return type;
    }

    public void setType(EsClient.Type type) {
        this.type = type;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public void setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
    }

    public boolean isRefresh() {
        return refresh;
    }

    public void setRefresh(boolean refresh) {
        this.refresh = refresh;
    }
}
