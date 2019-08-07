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
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/6.3/docs-refresh.html">the refresh policy</a>
     */
    protected WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.NONE;

    /**
     * Should a refresh be executed before this get operation causing the operation to
     * return the latest value. Note, heavy get should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    protected boolean refresh = false;


    public EsClient(Closeable client) {
        this.client = client;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setRefreshPolicy(String refresh) {
        this.refreshPolicy = WriteRequest.RefreshPolicy.parse(refresh);
    }

    public String getRefreshPolicy() {
        return refreshPolicy.getValue();
    }

    public void setRefresh(boolean refresh) {
        this.refresh = refresh;
    }

    public boolean getRefresh() {
        return this.refresh;
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
        @Deprecated
        REST,
        REST_HIGH_LEVEL
    }

}
