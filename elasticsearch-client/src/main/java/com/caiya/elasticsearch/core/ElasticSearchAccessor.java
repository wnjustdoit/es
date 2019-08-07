package com.caiya.elasticsearch.core;

import com.caiya.elasticsearch.EsClient;
import org.elasticsearch.action.support.WriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Base class for {@link ElasticSearchTemplate} defining common properties. Not intended to be used directly.
 *
 * @author wangnan
 * @since 1.0
 */
public abstract class ElasticSearchAccessor {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private Map<String, String> settings;

    private List<String> clusters;

    private WriteRequest.RefreshPolicy refreshPolicy;

    private boolean refresh;

    private EsClient.Type clientType = EsClient.Type.REST_HIGH_LEVEL;

    public void setSettings(Map<String, String> settings) {
        this.settings = settings;
    }

    public void setClusters(List<String> clusters) {
        if (clusters == null || clusters.isEmpty())
            throw new IllegalArgumentException("ElasticSearch clusters config is required");

        this.clusters = clusters;
    }

    protected Map<String, String> getSettings() {
        return settings;
    }

    protected List<String> getClusters() {
        return clusters;
    }

    public void setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public void setRefresh(boolean refresh) {
        this.refresh = refresh;
    }

    protected boolean getRefresh() {
        return refresh;
    }

    public void setClientType(EsClient.Type clientType) {
        this.clientType = clientType;
    }

    protected EsClient.Type getClientType() {
        return clientType;
    }

    /**
     * @return a new elasticsearch client.
     */
    abstract EsClient getClient();
}
