package com.caiya.elasticsearch.core;

import com.caiya.elasticsearch.ElasticSearchException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.caiya.elasticsearch.EsClient;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * ElasticSearch client wrapper, based on traditional transport client.
 *
 * @author wangnan
 * @since 1.0
 */
public class ElasticSearchClient extends EsClient {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);

    /**
     * the original transport client
     */
    private final TransportClient client;

    ElasticSearchClient(TransportClient client) {
        super(client);

        if (client == null)
            throw new IllegalArgumentException("TransportClient cannot be null.");

        this.client = client;
    }

    @Override
    public boolean index(String index, String type, String id, String source) {
        IndexResponse response = client.prepareIndex(index, type, id)
                .setRefreshPolicy(getRefreshPolicy())
                .setSource(source, XContentType.JSON)
                .get();
        return RestStatus.OK.equals(response.status());
    }

    @Override
    public Map<String, Object> get(String index, String type, String id) {
        GetResponse response = client.prepareGet(index, type, id)
                .setRefresh(getRefresh())
                .get();
        return response.getSource();
    }

    @Override
    @Deprecated
    public boolean exists(String index, String type, String id) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public boolean delete(String index, String type, String id) {
        DeleteResponse response = client.prepareDelete(index, type, id)
                .setRefreshPolicy(getRefreshPolicy())
                .get();
        return RestStatus.OK.equals(response.status());
    }

    @Override
    public boolean update(String index, String type, String id, String source) {
        UpdateResponse response = client.prepareUpdate(index, type, id)
                .setRefreshPolicy(getRefreshPolicy())
                .setDoc(source, XContentType.JSON)
                .get();
        return RestStatus.OK.equals(response.status());
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
            response = client.update(updateRequest).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("upsert failed");
            throw new ElasticSearchException(e);
        }
        return RestStatus.OK.equals(response.status()) || RestStatus.CREATED.equals(response.status());
    }

    @Override
    public List<Map<String, Object>> multiGet(String index, String type, String... ids) {
        List<Map<String, Object>> result = Lists.newArrayList();
        MultiGetResponse responses = client.prepareMultiGet()
                .add(index, type, ids)
                .setRefresh(getRefresh())
                .get();
        for (MultiGetItemResponse itemResponse : responses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                Map<String, Object> source = response.getSource();
                result.add(source);
            }
        }
        return result;
    }

    @Override
    public Map<String, Map<String, Object>> gets(String index, String type, String... ids) {
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.idsQuery().addIds(ids))
                .setFrom(0)
                .setSize(ids.length)
                .get();
        Map<String, Map<String, Object>> result = Maps.newHashMap();
        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> row = searchHit.getSourceAsMap();
            result.put(searchHit.getId(), row);
        }
        return result;
    }

    @Override
    public long deleteByQuery(QueryBuilder queryBuilder, String index) {
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .refresh(getRefresh())
                .filter(queryBuilder)
                .source(index)
                .get();
        return response.getDeleted();
    }


    @Override
    public void bulk(List<IndexRequest> indexRequests, List<IndexRequestBuilder> indexRequestBuilders, List<UpdateRequest> updateRequests, List<UpdateRequestBuilder> updateRequestBuilders, List<DeleteRequest> deleteRequests, List<DeleteRequestBuilder> deleteRequestBuilders) {
        BulkRequestBuilder bulkRequest = client.prepareBulk()
                .setRefreshPolicy(getRefreshPolicy());
        if (indexRequests != null && !indexRequests.isEmpty()) {
            for (IndexRequest indexRequest : indexRequests) {
                bulkRequest.add(indexRequest);
            }
        }
        if (indexRequestBuilders != null && !indexRequestBuilders.isEmpty()) {
            for (IndexRequestBuilder indexRequestBuilder : indexRequestBuilders) {
                bulkRequest.add(indexRequestBuilder);
            }
        }
        if (updateRequests != null && !updateRequests.isEmpty()) {
            for (UpdateRequest updateRequest : updateRequests) {
                bulkRequest.add(updateRequest);
            }
        }
        if (updateRequestBuilders != null && !updateRequestBuilders.isEmpty()) {
            for (UpdateRequestBuilder updateRequestBuilder : updateRequestBuilders) {
                bulkRequest.add(updateRequestBuilder);
            }
        }
        if (deleteRequests != null && !deleteRequests.isEmpty()) {
            for (DeleteRequest deleteRequest : deleteRequests) {
                bulkRequest.add(deleteRequest);
            }
        }
        if (deleteRequestBuilders != null && !deleteRequestBuilders.isEmpty()) {
            for (DeleteRequestBuilder deleteRequestBuilder : deleteRequestBuilders) {
                bulkRequest.add(deleteRequestBuilder);
            }
        }
        BulkResponse responses = bulkRequest.get();
        if (responses.hasFailures()) {
            BulkItemResponse[] itemResponses = responses.getItems();
            logger.error("bulk failed, items:{}", ReflectionToStringBuilder.toString(itemResponses));
            throw new ElasticSearchException("bulk failed");
        }
    }

    @Override
    public void bulkIndex(String index, String type, Map<String, String> idSources) {
        List<IndexRequest> indexRequests = Lists.newArrayList();
        for (Map.Entry<String, String> idSource : idSources.entrySet()) {
            IndexRequest indexRequest = new IndexRequest(index, type, idSource.getKey())
                    .source(idSource.getValue(), XContentType.JSON)
                    .setRefreshPolicy(getRefreshPolicy());
            indexRequests.add(indexRequest);
        }
        bulk(indexRequests, null, null, null, null, null);
    }

    @Override
    public void bulkDelete(String index, String type, List<String> ids) {
        List<DeleteRequest> deleteRequests = Lists.newArrayList();
        for (String id : ids) {
            DeleteRequest deleteRequest = new DeleteRequest(index, type, id).setRefreshPolicy(getRefreshPolicy());
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
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(queryBuilder)
                .setFrom(from)
                .setSize(size)
                .get();
        return response.getHits();
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
        UpdateByQueryRequestBuilder updateByQuery = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
        BulkByScrollResponse response = updateByQuery.source(index)
                .filter(queryBuilder)
                .size(1000)
                .script(new Script(ScriptType.INLINE, script, "painless", Collections.emptyMap()))
                .refresh(getRefresh())
                .get();
        return response.getUpdated();
    }

    @Override
    public SearchResponse scroll(String index, QueryBuilder queryBuilder, String sortField, SortOrder sortOrder, int size) {
        return client.prepareSearch(index)
                .addSort(sortField, sortOrder)
                .setScroll(new TimeValue(60000))// timeout
                .setQuery(queryBuilder)
                .setSize(size)
                .get();
    }

    @Override
    public SearchResponse scroll(String scrollId) {
        return client.prepareSearchScroll(scrollId)
                .setScroll(new TimeValue(60000))
                .execute()
                .actionGet();
    }


}
