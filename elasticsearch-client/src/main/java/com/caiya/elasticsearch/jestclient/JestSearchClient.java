package com.caiya.elasticsearch.jestclient;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.caiya.elasticsearch.ElasticSearchException;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;
import io.searchbox.core.search.sort.Sort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Elasticsearch client based on Jest.
 * <p>
 * 默认查询前、更新后都不做同步内存到磁盘的持久化操作；
 * 如有需要，可通过带有refresh参数的方法进行实时传参调用
 * </p>
 */
public class JestSearchClient {

    private static final Logger logger = LoggerFactory.getLogger(JestSearchClient.class);

    private final JestClient client;

    JestSearchClient(JestClientFactory jestClientFactory) {
        this.client = jestClientFactory.getObject();
    }

    public static JestSearchClientBuilder builder() {
        return new JestSearchClientBuilder();
    }

    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            logger.error("jedis close failed", e);
        }
    }

    public boolean index(String index, String type, String id, String source) {
        return index(index, type, id, source, false);
    }

    public boolean index(String index, String type, String id, String source, boolean refresh) {
        Index idx = new Index.Builder(source).index(index).type(type).id(id).refresh(refresh).build();
        try {
            return this.client.execute(idx).isSucceeded();
        } catch (IOException e) {
            logger.error("index failed, index:{}, type:{}, id:{}, source:{}", index, type, id, source, e);
            throw new ElasticSearchException("index failed");
        }
    }

    public Map get(String index, String type, String id) {
        return get(index, type, id, false);
    }

    public Map get(String index, String type, String id, boolean refresh) {
        Get get = new Get.Builder(index, id).type(type).refresh(refresh).build();
        try {
            return this.client.execute(get).getSourceAsObject(Map.class);
        } catch (IOException e) {
            logger.error("get failed, index:{}, type:{}, id:{}", index, type, id, e);
            throw new ElasticSearchException("get failed");
        }
    }

    public boolean delete(String index, String type, String id) {
        return delete(index, type, id, false);
    }

    public boolean delete(String index, String type, String id, boolean refresh) {
        Delete delete = new Delete.Builder(id).index(index).type(type).id(id).refresh(refresh).build();
        try {
            return this.client.execute(delete).isSucceeded();
        } catch (IOException e) {
            logger.error("delete failed, index:{}, type:{}, id:{}", index, type, id, e);
            throw new ElasticSearchException("delete failed");
        }
    }

    public boolean update(String index, String type, String id, String source) {
        return update(index, type, id, source, false);
    }

    public boolean update(String index, String type, String id, String source, boolean refresh) {
        JSONObject doc = new JSONObject();
        doc.put("doc", JSON.parseObject(source));
        Update update = new Update.Builder(doc).index(index).type(type).id(id).refresh(refresh).build();
        try {
            return this.client.execute(update).isSucceeded();
        } catch (IOException e) {
            logger.error("update failed, index:{}, type:{}, id:{}, source:{}", index, type, id, source, e);
            throw new ElasticSearchException("update failed");
        }
    }

    public List<Map> multiGet(String index, String type, Collection<String> ids) {
        return multiGet(index, type, ids, false);
    }

    public List<Map> multiGet(String index, String type, Collection<String> ids, boolean refresh) {
        MultiGet multiGet = new MultiGet.Builder.ById(index, type).addId(ids).refresh(refresh).build();
        try {
            return this.client.execute(multiGet).getSourceAsObjectList(Map.class);
        } catch (IOException e) {
            logger.error("multiGet failed, index:{}, type:{}, ids:{}", index, type, ids, e);
            throw new ElasticSearchException("multiGet failed");
        }
    }

    public boolean deleteByQuery(String query, String index, String type) {
        return deleteByQuery(query, index, type, false);
    }

    public boolean deleteByQuery(String query, String index, String type, boolean refresh) {
        DeleteByQuery deleteByQuery = new DeleteByQuery.Builder(query).addIndex(index).addType(type).refresh(refresh).build();
        try {
            return this.client.execute(deleteByQuery).isSucceeded();
        } catch (IOException e) {
            logger.error("deleteByQuery failed, index:{}, type:{}, query:{}", index, type, query, e);
            throw new ElasticSearchException("deleteByQuery failed");
        }
    }

    public BulkResult bulk(String defaultIndex, String defaultType, Collection<? extends BulkableAction> actions) {
        return bulk(defaultIndex, defaultType, actions, false);
    }

    public BulkResult bulk(String defaultIndex, String defaultType, Collection<? extends BulkableAction> actions, boolean refresh) {
        Bulk bulk = new Bulk.Builder().defaultIndex(defaultIndex).defaultType(defaultType).addAction(actions).refresh(refresh).build();
        try {
            BulkResult result = client.execute(bulk);
            if (!result.isSucceeded()) {
                logger.error("bulk failed, defaultIndex:{}, defaultType:{}, failedItems:{}", defaultIndex, defaultType, result.getFailedItems());
            }
            return result;
        } catch (IOException e) {
            logger.error("bulk failed, defaultIndex:{}, defaultType:{}, actions:{}", defaultIndex, defaultType, actions, e);
            throw new ElasticSearchException("bulk failed");
        }
    }

    public SearchResult search(String index, String type, String query) {
        return search(index, type, query, null, false);
    }

    @Deprecated
    private SearchResult search(String index, String type, String query, boolean refresh) {
        return search(index, type, query, null, refresh);
    }

    public SearchResult search(String index, String type, String query, Collection<Sort> sorts) {
        return search(index, type, query, sorts, false);
    }

    private SearchResult search(String index, String type, String query, Collection<Sort> sorts, boolean refresh) {
        Search search = new Search.Builder(query).addIndex(index).addType(type)
                .addSort(sorts == null ? Collections.emptyList() : sorts)
//                .refresh(refresh)
                .build();
        try {
            return this.client.execute(search);
        } catch (IOException e) {
            logger.error("search failed, index:{}, type:{}, query:{}", index, type, query, e);
            throw new ElasticSearchException("search failed");
        }
    }

    public MultiSearchResult multiSearch(Collection<? extends Search> searches) {
        return multiSearch(searches, false);
    }

    private MultiSearchResult multiSearch(Collection<? extends Search> searches, boolean refresh) {
        MultiSearch multiSearch = new MultiSearch.Builder(searches)
//                .refresh(refresh)
                .build();
        try {
            return client.execute(multiSearch);
        } catch (IOException e) {
            logger.error("multiSearch failed, searches:{}", searches, e);
            throw new ElasticSearchException("multiSearch failed");
        }
    }

    public long updateByQuery(String query, String index, String type) {
        return updateByQuery(query, index, type, false);
    }

    public long updateByQuery(String query, String index, String type, boolean refresh) {
        UpdateByQuery updateByQuery = new UpdateByQuery.Builder(query).refresh(refresh).addIndex(index).addType(type).build();
        try {
            UpdateByQueryResult updateByQueryResult = client.execute(updateByQuery);
            if (!updateByQueryResult.isSucceeded()) {
                logger.error("updateByQuery failed, query:{} index:{}, type:{}", query, index, type);
            }
            return updateByQueryResult.getUpdatedCount();
        } catch (IOException e) {
            logger.error("updateByQuery failed, index:{}, type:{}, query:{}", index, type, query, e);
            throw new ElasticSearchException("updateByQuery failed");
        }
    }

    public JestResult searchScroll(Search search) {
        try {
            return client.execute(search);
        } catch (IOException e) {
            logger.error("searchScroll failed, search:{}", search, e);
            throw new ElasticSearchException("searchScroll failed0");
        }
    }

    public JestResult searchScroll(String scrollId) {
        return searchScroll(scrollId, false);
    }

    public JestResult searchScroll(String scrollId, boolean refresh) {
        SearchScroll searchScroll = new SearchScroll.Builder(scrollId, "1m")
//                .refresh(refresh)
                .build();
        try {
            return client.execute(searchScroll);
        } catch (IOException e) {
            logger.error("searchScroll failed, scrollId:{}, refresh:{}", scrollId, refresh, e);
            throw new ElasticSearchException("searchScroll failed1");
        }
    }

    public boolean clearScroll(String scrollId) {
        try {
            return client.execute(new ClearScroll.Builder().addScrollId(scrollId).build()).isSucceeded();
        } catch (IOException e) {
            logger.error("clearScroll failed, scrollId:{}", scrollId, e);
            throw new ElasticSearchException("clearScroll failed");
        }
    }


}
