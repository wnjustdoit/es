package com.caiya.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Ignore
public class BaseElasticSearchTest {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected ElasticSearchApi elasticSearchClient;

    private String index = "item";

    private String type = "item";

    private String id = "80003684";

    private String id1 = "80003688";

    private String id2 = "80003690";

    @Test
    @Ignore
    public void test_a_Parallel() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(1000);
        for (int i = 0; i < 10000; i++) {
            final int finalI = i;
            executorService.execute(new Thread(() -> {
                String jsonStr = "{\"updated\":1534471509,\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521769233717697303.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":53,\"popularity\":0,\"description\":\"\",\"id\":" + finalI + ",\"title\":\"德国施巴儿童洗发液150ml\"}";
                boolean result = false;
                try {
                    result = elasticSearchClient.upsert(index, type, finalI + "", jsonStr);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                Assert.assertTrue(result);
            }));
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }

    @Test
    public void test_A_index() {
        String jsonStr = "{\"updated\":1534471509,\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521769233717697303.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":53,\"popularity\":0,\"description\":\"\",\"id\":80003684,\"title\":\"德国施巴儿童洗发液150ml\"}";
        boolean result = elasticSearchClient.index(index, type, id, jsonStr);
        Assert.assertTrue(result);
    }

    @Test
    public void test_B_get() {
        Map<String, Object> result = elasticSearchClient.get(index, type, id);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test
    public void test_C_update() {
        boolean result = elasticSearchClient.update(index, type, id, "{\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521769233717697303.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":53,\"popularity\":0,\"description\":\"\",\"id\":80003684,\"title\":\"德国施巴儿童洗发液150ml.\"}");
        Assert.assertTrue(result);
    }

    @Test
    public void test_D_upsert() {
        boolean result = elasticSearchClient.upsert(index, type, id, "{\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521769233717697303.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":53,\"popularity\":0,\"description\":\"\",\"id\":80003684,\"title\":\"德国施巴儿童洗发液150ml.\"}");
        Assert.assertTrue(result);
    }

    @Test
    public void test_Z1_delete() {
        boolean result = elasticSearchClient.delete(index, type, id);
        Assert.assertTrue(result);
    }

    @Test
    public void test_Z2_deleteByQuery() {
        elasticSearchClient.upsert(index, type, id, "{\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521769233717697303.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":53,\"popularity\":0,\"description\":\"\",\"id\":80003684,\"title\":\"德国施巴儿童洗发液150ml.\"}");
        long result = elasticSearchClient.deleteByQuery(QueryBuilders.idsQuery(type).addIds(id), index);
        Assert.assertTrue(result > 0);
    }

    @Test
    public void test_E_multiGet() {
        List<Map<String, Object>> result = elasticSearchClient.multiGet(index, type, id, id1, id2);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test
    public void test_F_bulkIndex() {
        String jsonStr = "[{\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521770774711925277.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":33.5,\"popularity\":0,\"description\":\"\",\"id\":80003688,\"title\":\"德国施巴婴儿洁肤皂100g\"},{\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521771066136948639.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":49.5,\"popularity\":0,\"description\":\"\",\"id\":80003690,\"title\":\"德国施巴婴儿泡泡浴露200ml\"}]";
        List<Map<String, Object>> list = JSON.parseObject(jsonStr, new TypeReference<List<Map<String, Object>>>() {
        });
        Map<String, String> idSources = Maps.newHashMap();
        for (Map<String, Object> row : list) {
            idSources.put(row.get("id").toString(), JSON.toJSONString(row));
        }
        elasticSearchClient.bulkIndex(index, type, idSources);
    }

    @Test
    public void test_Z0_bulkDelete() {
        List<String> ids = Lists.newArrayList(id1, id2);
        elasticSearchClient.bulkDelete(index, type, ids);
    }


    @Test
    public void test_G_matchQuery() {
//        QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery("奶粉", "title", "brand")).filter(QueryBuilders.termQuery("sendWay", "xxx"));
        SearchHits searchHits = elasticSearchClient.matchQuery(index, type, "title", "德", 0, 2);
        Assert.assertNotNull(searchHits);
        Assert.assertTrue(searchHits.getTotalHits() > 0);
        Assert.assertNotNull(searchHits.getHits());
        Assert.assertTrue(searchHits.getHits().length > 0);
    }

    @Test
    public void test_H_scroll() {
        Map<String, Map<String, Object>> result = Maps.newHashMap();
        int batchSize = 500;
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        SearchResponse scrollResponse = elasticSearchClient.scroll(index, queryBuilder, "updated", SortOrder.DESC, batchSize);
        appendToResult(scrollResponse, result);
        String scrollId = scrollResponse.getScrollId();
        try {
            while (scrollResponse.getHits().getHits().length > 0) {
                scrollResponse = elasticSearchClient.scroll(scrollResponse.getScrollId());
                appendToResult(scrollResponse, result);
            }
        } finally {
            boolean clearResult = elasticSearchClient.clearScroll(scrollId);
            Assert.assertTrue(clearResult);
        }
        Assert.assertEquals(result.size(), scrollResponse.getHits().getTotalHits());
    }

    private void appendToResult(SearchResponse scrollResponse, Map<String, Map<String, Object>> result) {
        for (SearchHit searchHit : scrollResponse.getHits().getHits()) {
            result.put(searchHit.getId(), searchHit.getSourceAsMap());
        }
    }

}
