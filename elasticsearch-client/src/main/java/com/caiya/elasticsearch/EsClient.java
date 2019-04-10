package com.caiya.elasticsearch;

import org.elasticsearch.action.support.WriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * es abstract client.
 *
 * @author wangnan
 * @since 1.0
 */
public abstract class EsClient implements ElasticSearchApi, Closeable {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * es client name
     */
    private String name;

    /**
     * the original client
     */
    private Closeable client;

    /**
     * the refresh policy, @see https://www.elastic.co/guide/en/elasticsearch/reference/6.3/docs-refresh.html
     */
    protected WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.NONE;


    public EsClient(Closeable client) {
        this.client = client;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }


    public void setRefresh(String refresh) {
        this.refreshPolicy = WriteRequest.RefreshPolicy.parse(refresh);
    }

    public String getRefresh() {
        return refreshPolicy.getValue();
    }

    @Override
    public void close() {
        try {
            if (client != null)
                client.close();
        } catch (Exception e) {
            logger.error("the original client close failed", e);
        }
    }

    public enum Type {
        TRANSPORT,
        REST,
        REST_HIGH_LEVEL
    }

}
