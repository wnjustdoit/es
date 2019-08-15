package com.caiya.elasticsearch.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.caiya.elasticsearch.ElasticSearchException;
import com.caiya.elasticsearch.EsClient;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * ElasticSearch client wrapper, based on high-level rest client.
 *
 * @author wangnan
 * @since 1.0
 */
public class RestElasticSearchClient extends EsClient {

    private static final Logger logger = LoggerFactory.getLogger(RestElasticSearchClient.class);

    /**
     * the original high-level rest client
     */
    private final RestHighLevelClient client;

    RestElasticSearchClient(RestHighLevelClient client) {
        super(client);

        if (client == null)
            throw new IllegalArgumentException("rest client cannot be null.");

        this.client = client;
    }


    @Override
    public boolean index(String index, String type, String id, String source) {
        IndexResponse response;
        try {
            response = client.index(new IndexRequest(index, type, id)
                    .setRefreshPolicy(getRefreshPolicy())
                    .source(source, XContentType.JSON));
            return RestStatus.OK.equals(response.status()) || RestStatus.CREATED.equals(response.status());
        } catch (IOException e) {
            logger.error("index failed, index:{}, type:{}, id:{}, source:{}", index, type, id, source, e);
            throw new ElasticSearchException(e);
        }
    }

    @Override
    public Map<String, Object> get(String index, String type, String id) {
        GetResponse response;
        try {
            response = client.get(new GetRequest(index, type, id).refresh(getRefresh()));
            return response.getSource();
        } catch (IOException e) {
            logger.error("get failed, index:{}, type:{}, id:{}", index, type, id, e);
            throw new ElasticSearchException(e);
        }
    }

    @Override
    public boolean exists(String index, String type, String id) {
        try {
            return client.exists(new GetRequest(index, type, id).refresh(getRefresh()));
        } catch (IOException e) {
            logger.error("exists failed, index:{}, type:{}, id:{}", index, type, id, e);
            throw new ElasticSearchException(e);
        }
    }

    @Override
    public boolean delete(String index, String type, String id) {
        DeleteResponse response;
        try {
            response = client.delete(new DeleteRequest(index, type, id)
                    .setRefreshPolicy(getRefreshPolicy()));
            return RestStatus.OK.equals(response.status());
        } catch (IOException e) {
            logger.error("delete failed, index:{}, type:{}, id:{}", index, type, id, e);
            throw new ElasticSearchException(e);
        }
    }

    @Override
    public boolean update(String index, String type, String id, String source) {
        UpdateResponse response;
        try {
            response = client.update(new UpdateRequest(index, type, id)
                    .setRefreshPolicy(getRefreshPolicy())
                    .doc(source, XContentType.JSON));
            return RestStatus.OK.equals(response.status());
        } catch (IOException e) {
            logger.error("update failed, index:{}, type:{}, id:{}, source:{}", index, type, id, source, e);
            throw new ElasticSearchException(e);
        }
    }

    @Override
    public boolean upsert(String index, String type, String id, String source) {
        IndexRequest indexRequest = new IndexRequest(index, type, id)
                .setRefreshPolicy(getRefreshPolicy())
                .source(source, XContentType.JSON);
        UpdateRequest updateRequest = new UpdateRequest(index, type, id)
                .setRefreshPolicy(getRefreshPolicy())
                .doc(source, XContentType.JSON)
                .upsert(indexRequest);
        UpdateResponse response;
        try {
            response = client.update(updateRequest);
            return RestStatus.OK.equals(response.status()) || RestStatus.CREATED.equals(response.status());
        } catch (IOException e) {
            logger.error("upsert failed, index:{}, type:{}, id:{}, source:{}", index, type, id, source, e);
            throw new ElasticSearchException(e);
        }
    }

    @Override
    public List<Map<String, Object>> multiGet(String index, String type, String... ids) {
        List<Map<String, Object>> result = Lists.newArrayList();
        MultiGetRequest request = new MultiGetRequest().refresh(getRefresh());
        for (String id : ids) {
            request.add(index, type, id);
        }
        MultiGetResponse responses;
        try {
            responses = client.multiGet(request);
            for (MultiGetItemResponse itemResponse : responses) {
                GetResponse response = itemResponse.getResponse();
                if (response.isExists()) {
                    Map<String, Object> source = response.getSource();
                    result.add(source);
                }
            }
            return result;
        } catch (IOException e) {
            logger.error("multiGet failed, index:{}, type:{}, ids:{}", index, type, ids, e);
            throw new ElasticSearchException(e);
        }
    }

    @Override
    public Map<String, Map<String, Object>> gets(String index, String type, String... ids) {
        SearchResponse response;
        try {
            response = client.search(new SearchRequest(index).types(type)
                    .source(SearchSourceBuilder.searchSource().query(QueryBuilders.idsQuery().addIds(ids))
                            .from(0)
                            .size(ids.length)));
            Map<String, Map<String, Object>> result = Maps.newHashMap();
            for (SearchHit searchHit : response.getHits().getHits()) {
                Map<String, Object> row = searchHit.getSourceAsMap();
                result.put(searchHit.getId(), row);
            }
            return result;
        } catch (IOException e) {
            logger.error("gets failed, index:{}, type:{}, ids:{}", index, type, ids, e);
            throw new ElasticSearchException(e);
        }
    }

    @Override
    @Deprecated
    public long deleteByQuery(QueryBuilder queryBuilder, String index) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public void bulk(List<IndexRequest> indexRequests, List<IndexRequestBuilder> indexRequestBuilders, List<UpdateRequest> updateRequests, List<UpdateRequestBuilder> updateRequestBuilders, List<DeleteRequest> deleteRequests, List<DeleteRequestBuilder> deleteRequestBuilders) {
        BulkRequest bulkRequest = new BulkRequest()
                .setRefreshPolicy(getRefreshPolicy());
        if (indexRequests != null && !indexRequests.isEmpty()) {
            for (IndexRequest indexRequest : indexRequests) {
                bulkRequest.add(indexRequest);
            }
        }
        if (indexRequestBuilders != null && !indexRequestBuilders.isEmpty()) {
            for (IndexRequestBuilder indexRequestBuilder : indexRequestBuilders) {
                bulkRequest.add(indexRequestBuilder.request());
            }
        }
        if (updateRequests != null && !updateRequests.isEmpty()) {
            for (UpdateRequest updateRequest : updateRequests) {
                bulkRequest.add(updateRequest);
            }
        }
        if (updateRequestBuilders != null && !updateRequestBuilders.isEmpty()) {
            for (UpdateRequestBuilder updateRequestBuilder : updateRequestBuilders) {
                bulkRequest.add(updateRequestBuilder.request());
            }
        }
        if (deleteRequests != null && !deleteRequests.isEmpty()) {
            for (DeleteRequest deleteRequest : deleteRequests) {
                bulkRequest.add(deleteRequest);
            }
        }
        if (deleteRequestBuilders != null && !deleteRequestBuilders.isEmpty()) {
            for (DeleteRequestBuilder deleteRequestBuilder : deleteRequestBuilders) {
                bulkRequest.add(deleteRequestBuilder.request());
            }
        }

        BulkResponse responses;
        try {
            responses = client.bulk(bulkRequest);
            if (responses.hasFailures()) {
                BulkItemResponse[] itemResponses = responses.getItems();
                logger.error("bulk failed, items:{}", ReflectionToStringBuilder.toString(itemResponses));
                throw new ElasticSearchException("bulk failed");
            }
        } catch (IOException e) {
            logger.error("bulk failed, ...", e);
            throw new ElasticSearchException(e);
        }
    }

    @Override
    public void bulkIndex(String index, String type, Map<String, String> idSources) {
        List<IndexRequest> indexRequests = Lists.newArrayList();
        for (Map.Entry<String, String> idSource : idSources.entrySet()) {
            IndexRequest indexRequest = new IndexRequest(index, type, idSource.getKey())
                    .source(idSource.getValue(), XContentType.JSON);
            indexRequests.add(indexRequest);
        }
        bulk(indexRequests, null, null, null, null, null);
    }

    @Override
    public void bulkDelete(String index, String type, List<String> ids) {
        List<DeleteRequest> deleteRequests = Lists.newArrayList();
        for (String id : ids) {
            DeleteRequest deleteRequest = new DeleteRequest(index, type, id);
            deleteRequests.add(deleteRequest);
        }
        bulk(null, null, null, null, deleteRequests, null);
    }

    @Override
    public void bulkUpdate(String index, String type, Map<String, String> idSources) {
        List<UpdateRequest> updateRequests = Lists.newArrayList();
        for (Map.Entry<String, String> idSource : idSources.entrySet()) {
            UpdateRequest updateRequest = new UpdateRequest(index, type, idSource.getKey())
                    .doc(idSource.getValue(), XContentType.JSON)
                    .setRefreshPolicy(getRefreshPolicy());
            updateRequests.add(updateRequest);
        }
        bulk(null, null, updateRequests, null, null, null);
    }

    @Override
    public void bulkUpsert(String index, String type, Map<String, String> idSources) {
        List<UpdateRequest> updateRequests = Lists.newArrayList();
        for (Map.Entry<String, String> idSource : idSources.entrySet()) {
            IndexRequest indexRequest = new IndexRequest(index, type, idSource.getKey())
                    .source(idSource.getValue(), XContentType.JSON)
                    .setRefreshPolicy(getRefreshPolicy());
            UpdateRequest updateRequest = new UpdateRequest(index, type, idSource.getKey())
                    .doc(idSource.getValue(), XContentType.JSON)
                    .upsert(indexRequest)
                    .setRefreshPolicy(getRefreshPolicy());
            updateRequests.add(updateRequest);
        }
        bulk(null, null, updateRequests, null, null, null);
    }

    @Override
    public SearchHits search(String index, String type, QueryBuilder queryBuilder, int from, int size) {
        SearchResponse response;
        try {
            response = client.search(new SearchRequest(index)
                    .types(type)
                    .source(SearchSourceBuilder.searchSource().query(queryBuilder)
                            .from(from)
                            .size(size)));
            return response.getHits();
        } catch (IOException e) {
            logger.error("search failed, index:{}, type:{}, queryBuilder:{}, from:{}, size:{}",
                    index, type, queryBuilder, from, size, e);
            throw new ElasticSearchException(e);
        }
    }

    @Override
    public SearchHits matchQuery(String index, String type, String name, Object text, String analyzer, int from, int size) {
        return search(index, type, QueryBuilders.matchQuery(name, text).operator(Operator.AND).analyzer(analyzer), from, size);
    }

    @Override
    public SearchHits matchQuery(String index, String type, String name, Object text, int from, int size) {
        return matchQuery(index, type, name, text, "ik_smart", from, size);
    }

    @Override
    public long updateByQuery(String index, QueryBuilder queryBuilder, String script) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public SearchResponse scroll(String index, QueryBuilder queryBuilder, String sortField, SortOrder sortOrder, int size) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(queryBuilder)
                .sort(sortField, sortOrder)
                .size(size);
        SearchRequest searchRequest = new SearchRequest(index)
                .source(searchSourceBuilder)
                .scroll(TimeValue.timeValueMinutes(1));
        try {
            return client.search(searchRequest);
        } catch (IOException e) {
            logger.error("scroll failed, index:{}, queryBuilder:{}, sortField:{}, sortOrder:{}, size:{}"
                    , index, queryBuilder, sortField, sortOrder, size, e);
            throw new ElasticSearchException(e);
        }
    }

    @Override
    public SearchResponse scroll(String scrollId) {
        try {
            return client.searchScroll(new SearchScrollRequest(scrollId)
                    .scroll(TimeValue.timeValueMinutes(1)));
        } catch (IOException e) {
            logger.error("scroll failed, scrollId:{}", scrollId, e);
            throw new ElasticSearchException(e);
        }
    }

    @Override
    public boolean clearScroll(String scrollId) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        try {
            return client.clearScroll(clearScrollRequest).isSucceeded();
        } catch (IOException e) {
            logger.error("clearScroll failed, scrollId:{}", scrollId, e);
            throw new ElasticSearchException(e);
        }
    }
}
