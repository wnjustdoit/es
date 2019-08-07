package com.caiya.elasticsearch.core;

import com.caiya.elasticsearch.ElasticSearchException;
import com.caiya.elasticsearch.EsClient;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * ElasticSearchTemplate based on spring-data-elasticsearch.
 * <p>
 * Helper class that simplifies ElasticSearch data access code.
 * <p>
 * Notice that the port(s) of traditional transport client is/are different from that/those of rest client
 * (or rest high level client). Choose the right pairs and enjoy fun.
 *
 * @author wangnan
 * @since 1.0
 */
public class ElasticSearchTemplate extends ElasticSearchAccessor implements ElasticSearchOperations {


    public ElasticSearchTemplate() {
    }


    @Override
    public EsClient getClient() {
        if (getClusters() == null)
            throw new IllegalArgumentException("clusters cannot be null");

        if (Objects.equals(getClientType(), EsClient.Type.TRANSPORT)) {
            return ElasticSearchClientBuilder.create(getSettings(), getClusters())
                    .setRefresh(getRefresh())
                    .build();
        } else if (Objects.equals(getClientType(), EsClient.Type.REST_HIGH_LEVEL)) {
            return RestElasticSearchClientBuilder.create(getClusters())
                    .setRefresh(getRefresh())
                    .build();
        }
        throw new IllegalArgumentException("Unsupported client type:" + getClientType());
    }

    private void close(Closeable client) {
        if (client != null)
            try {
                client.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
    }

    public <T> T execute(ElasticSearchCallback<T> callback) {
        EsClient client = null;
        try {
            client = getClient();
            return callback.doInElasticSearch(client);
        } finally {
            close(client);
        }
    }

    @Override
    public boolean index(String index, String type, String id, String source) {
        return execute(client -> client.index(index, type, id, source));
    }

    @Override
    public Map<String, Object> get(String index, String type, String id) {
        return execute(client -> client.get(index, type, id));
    }

    @Override
    @Deprecated
    public boolean exists(String index, String type, String id) {
        throw new UnsupportedOperationException("不支持exists操作");
    }

    @Override
    public boolean delete(String index, String type, String id) {
        return execute(client -> client.delete(index, type, id));
    }

    @Override
    public boolean update(String index, String type, String id, String source) {
        return execute(client -> client.update(index, type, id, source));
    }

    @Override
    public boolean upsert(String index, String type, String id, String source) {
        return execute(client -> {
            try {
                return client.upsert(index, type, id, source);
            } catch (Exception e) {
                logger.error("upsert failed, index:{}, type:{}, id:{}, source:{}", index, type, id, source, e);
                throw new ElasticSearchException(e);
            }
        });
    }

    @Override
    public List<Map<String, Object>> multiGet(String index, String type, String... ids) {
        return execute(client -> client.multiGet(index, type, ids));
    }

    @Override
    public Map<String, Map<String, Object>> gets(String index, String type, String... ids) {
        return execute(client -> client.gets(index, type, ids));
    }

    @Override
    public long deleteByQuery(QueryBuilder queryBuilder, String index) {
        return execute(client -> client.deleteByQuery(queryBuilder, index));
    }

    @Override
    public void bulk(List<IndexRequest> indexRequests, List<IndexRequestBuilder> indexRequestBuilders, List<UpdateRequest> updateRequests, List<UpdateRequestBuilder> updateRequestBuilders, List<DeleteRequest> deleteRequests, List<DeleteRequestBuilder> deleteRequestBuilders) {
        execute(client -> {
            client.bulk(indexRequests, indexRequestBuilders, updateRequests, updateRequestBuilders, deleteRequests, deleteRequestBuilders);
            return null;
        });
    }

    @Override
    public void bulkIndex(String index, String type, Map<String, String> idSources) {
        execute(client -> {
            client.bulkIndex(index, type, idSources);
            return null;
        });
    }

    @Override
    public void bulkDelete(String index, String type, List<String> ids) {
        execute(client -> {
            client.bulkDelete(index, type, ids);
            return null;
        });
    }

    @Override
    public void bulkUpdate(String index, String type, Map<String, String> idSources) {
        execute(client -> {
            client.bulkUpdate(index, type, idSources);
            return null;
        });
    }

    @Override
    public void bulkUpsert(String index, String type, Map<String, String> idSources) {
        execute(client -> {
            client.bulkUpsert(index, type, idSources);
            return null;
        });
    }

    @Override
    public SearchHits search(String index, String type, QueryBuilder queryBuilder, int from, int size) {
        return execute(client -> client.search(index, type, queryBuilder, from, size));
    }

    @Override
    public SearchHits matchQuery(String index, String type, String name, Object text, String analyzer, int from, int size) {
        return execute(client -> client.matchQuery(index, type, name, text, analyzer, from, size));
    }

    @Override
    public SearchHits matchQuery(String index, String type, String name, Object text, int from, int size) {
        return execute(client -> client.matchQuery(index, type, name, text, from, size));
    }

    @Override
    public long updateByQuery(String index, QueryBuilder queryBuilder, String script) {
        return execute(client -> client.updateByQuery(index, queryBuilder, script));
    }

    @Override
    public SearchResponse scroll(String index, QueryBuilder queryBuilder, String sortField, SortOrder sortOrder, int size) {
        return execute(client -> client.scroll(index, queryBuilder, sortField, sortOrder, size));
    }

    @Override
    public SearchResponse scroll(String scrollId) {
        return execute(client -> client.scroll(scrollId));
    }

}
