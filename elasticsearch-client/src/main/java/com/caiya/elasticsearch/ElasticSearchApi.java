package com.caiya.elasticsearch;

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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * ElasticSearch API.
 *
 * @author wangnan
 * @since 1.0
 */
public interface ElasticSearchApi {

    /**
     * 索引
     *
     * @param index  index
     * @param type   type
     * @param id     id
     * @param source json string
     * @return result
     */
    boolean index(String index, String type, String id, String source);

    /**
     * 查询
     *
     * @param index index
     * @param type  type
     * @param id    id
     * @return map result
     */
    Map<String, Object> get(String index, String type, String id);

    /**
     * 是否存在
     *
     * @param index index
     * @param type  type
     * @param id    id
     * @return true or false
     */
    boolean exists(String index, String type, String id);

    /**
     * 删除
     *
     * @param index index
     * @param type  type
     * @param id    id
     */
    boolean delete(String index, String type, String id);

    /**
     * 更新
     *
     * @param index  index
     * @param type   type
     * @param id     id
     * @param source json string
     */
    boolean update(String index, String type, String id, String source);

    /**
     * 更新或索引
     *
     * @param index  index
     * @param type   type
     * @param id     id
     * @param source json string
     */
    boolean upsert(String index, String type, String id, String source) throws ExecutionException, InterruptedException;

    /**
     * 批量查询
     *
     * @param index index
     * @param type  type
     * @param ids   ids
     * @return multi rows
     */
    List<Map<String, Object>> multiGet(String index, String type, String... ids);

    /**
     * 批量查询2
     *
     * @param index index
     * @param type  type
     * @param ids   ids
     * @return map kvs
     */
    Map<String, Map<String, Object>> gets(String index, String type, String... ids);

    /**
     * 根据查询删除
     *
     * @param queryBuilder queryBuilder
     * @param index        index
     * @return delete rows num
     */
    long deleteByQuery(QueryBuilder queryBuilder, String index);

    /**
     * 批量复合操作
     *
     * @param indexRequests         indexRequests
     * @param indexRequestBuilders  indexRequestBuilders
     * @param updateRequests        updateRequests
     * @param updateRequestBuilders updateRequestBuilders
     * @param deleteRequests        deleteRequests
     * @param deleteRequestBuilders deleteRequestBuilders
     */
    void bulk(List<IndexRequest> indexRequests, List<IndexRequestBuilder> indexRequestBuilders, List<UpdateRequest> updateRequests, List<UpdateRequestBuilder> updateRequestBuilders, List<DeleteRequest> deleteRequests, List<DeleteRequestBuilder> deleteRequestBuilders);

    /**
     * 批量索引
     *
     * @param index common index
     * @param type  common type
     */
    void bulkIndex(String index, String type, Map<String, String> idSources);

    /**
     * 批量删除
     *
     * @param index common index
     * @param type  common type
     * @param ids   ids
     */
    void bulkDelete(String index, String type, List<String> ids);

    /**
     * 批量更新
     *
     * @param index     common index
     * @param type      common type
     * @param idSources idSources
     */
    void bulkUpdate(String index, String type, Map<String, String> idSources);

    /**
     * 批量更新或插入
     *
     * @param index     common index
     * @param type      common type
     * @param idSources idSources
     */
    void bulkUpsert(String index, String type, Map<String, String> idSources);

    /**
     * 搜索基础
     *
     * @param index        index
     * @param type         type
     * @param queryBuilder queryBuilder
     * @return map
     */
    SearchHits search(String index, String type, QueryBuilder queryBuilder, int from, int size);

    /**
     * 搜索
     *
     * @param index    index
     * @param type     type
     * @param name     搜索名称
     * @param text     全文本
     * @param analyzer 分词器名称
     * @return map
     */
    SearchHits matchQuery(String index, String type, String name, Object text, String analyzer, int from, int size);

    /**
     * 搜索
     *
     * @param index index
     * @param type  type
     * @param name  搜索名称
     * @param text  全文本
     * @return map
     */
    SearchHits matchQuery(String index, String type, String name, Object text, int from, int size);

    /**
     * 根据搜索更新(一次最多更新1000条)
     * ctx._source
     *
     * @param index        index
     * @param queryBuilder queryBuilder
     * @param script       script
     * @return updated
     */
    long updateByQuery(String index, QueryBuilder queryBuilder, String script);

    /**
     * 滚动轮询,首次轮询
     *
     * @param index        index
     * @param queryBuilder queryBuilder
     * @param sortField    sort field
     * @param sortOrder    sort order
     * @param size         size
     * @return search response, include scrollId
     */
    SearchResponse scroll(String index, QueryBuilder queryBuilder, String sortField, SortOrder sortOrder, int size);

    /**
     * 根据scrollId滚动轮询,再次轮询
     *
     * @param scrollId scrollId
     * @return search response, include scrollId
     */
    SearchResponse scroll(String scrollId);


}
