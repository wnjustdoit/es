package com.caiya.elasticsearch.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.caiya.elasticsearch.jestclient.JestSearchClient;
import com.google.common.collect.Lists;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class JestSearchClientTest {

    private JestSearchClient client;

    private String index = "item";

    private String type = "item";

    private String id = "80003684";

    private String id1 = "80003688";

    private String id2 = "80003690";

    @Before
    public void before() {
        client = JestSearchClient.builder()
                .withHttpClientConfig(new HttpClientConfig
                        .Builder(Arrays.asList("http://127.0.0.1:9200", "http://127.0.0.1:9201"))
                        .multiThreaded(true)
                        //Per default this implementation will create no more than 2 concurrent connections per given route
                        .defaultMaxTotalConnectionPerRoute(2)
                        // and no more 20 connections in total
                        .maxTotalConnection(20)
                        .build())
                .build();
    }

    @Test
    @Ignore
    public void test_a_Parallel() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(1000);
        for (int i = 0; i < 100000; i++) {
            final int finalI = i;
            executorService.execute(new Thread(() -> {
                String jsonStr = "{\"updated\":1534471509,\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521769233717697303.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":53,\"popularity\":0,\"description\":\"\",\"id\":" + finalI + ",\"title\":\"德国施巴儿童洗发液150ml\"}";
                boolean result = false;
                try {
                    result = client.index(index, type, finalI + "", jsonStr, true);
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
        String jsonStr = "{\"updated\":1534471509,\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521769233717697303.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":53,\"popularity\":0,\"description\":\"这是一条描述\",\"id\":80003684,\"title\":\"德国施巴儿童洗发液150ml\"}";
        boolean result = client.index(index, type, id, jsonStr, true);
        Assert.assertTrue(result);
    }

    @Test
    public void test_B_get() {
        Map result = client.get(index, type, id);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test
    public void test_C_update() {
        boolean result = client.update(index, type, id, "{\"updated\":0,\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521769233717697303.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":53,\"popularity\":0,\"description\":\"\",\"id\":80003684,\"title\":\"德国施巴儿童洗发液150ml.\"}", true);
        Assert.assertTrue(result);
    }

    @Test
    public void test_Z1_delete() {
        boolean result = client.delete(index, type, id, true);
        Assert.assertTrue(result);
    }

    @Test
    public void test_Z2_deleteByQuery() {
        client.index(index, type, id, "{\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521769233717697303.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":53,\"popularity\":0,\"description\":\"\",\"id\":80003684,\"title\":\"德国施巴儿童洗发液150ml.\"}", true);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.termQuery("id", 80003684);
        searchSourceBuilder.query(queryBuilder);
        boolean result = client.deleteByQuery(searchSourceBuilder.toString(), index, type, true);
        Assert.assertTrue(result);
    }

    @Test
    public void test_E_multiGet() {
        List<Map> result = client.multiGet(index, type, Arrays.asList(id, id1, id2));
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test
    public void test_F_bulkIndex() {
        String jsonStr = "[{\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521770774711925277.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":33.5,\"popularity\":0,\"description\":\"\",\"id\":80003688,\"title\":\"德国施巴婴儿洁肤皂100g\"},{\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521771066136948639.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":49.5,\"popularity\":0,\"description\":\"\",\"id\":80003690,\"title\":\"德国施巴婴儿泡泡浴露200ml\"}]";
        List<Map<String, Object>> list = JSON.parseObject(jsonStr, new TypeReference<List<Map<String, Object>>>() {
        });
        List<Index> indexes = Lists.newArrayList();
        for (Map<String, Object> row : list) {
            Index idx = new Index.Builder(JSON.toJSONString(row)).index(index).type(type).id(row.get("id").toString()).refresh(true).build();
            indexes.add(idx);
        }
        BulkResult result = client.bulk(index, type, indexes);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isSucceeded());
    }

    @Test
    public void test_F_bulkUpdate() {
        String jsonStr = "[{\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521770774711925277.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":33.5,\"popularity\":0,\"description\":\"\",\"id\":80003688,\"title\":\"德国施巴婴儿洁肤皂100g111\"},{\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521771066136948639.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":49.5,\"popularity\":0,\"description\":\"\",\"id\":80003690,\"title\":\"德国施巴婴儿泡泡浴露200ml111\"}]";
        List<Map<String, Object>> list = JSON.parseObject(jsonStr, new TypeReference<List<Map<String, Object>>>() {
        });
        List<Update> updates = Lists.newArrayList();
        for (Map<String, Object> row : list) {
            Map<String, Map<String, Object>> doc = new HashMap<>();
            doc.put("doc", row);
            Update update = new Update.Builder(doc).index(index).type(type).id(row.get("id").toString()).refresh(true).build();
            updates.add(update);
        }
        BulkResult result = client.bulk(index, type, updates);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isSucceeded());
    }

    @Test
    public void test_Z0_bulkDelete() {
        List<String> ids = Lists.newArrayList(id1, id2);
        List<Delete> deletes = Lists.newArrayListWithCapacity(ids.size());
        for (String id : ids) {
            deletes.add(new Delete.Builder(id).index(index).type(type).id(id).refresh(true).build());
        }
        BulkResult result = client.bulk(index, type, deletes);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isSucceeded());
    }

    /**
     * {"error":{"root_cause":[{"type":"illegal_argument_exception","reason":"request [/item/item/_search] contains unrecognized parameter: [refresh]"}],"type":"illegal_argument_exception","reason":"request [/item/item/_search] contains unrecognized parameter: [refresh]"},"status":400}
     */
    @Test
    public void test_G1_search() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery("德国", "title", "brand")).filter(QueryBuilders.termQuery("sendWay", 1));
        searchSourceBuilder.sort("id", SortOrder.DESC);
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        SearchResult searchResult = client.search(index, type, searchSourceBuilder.toString());
        Assert.assertNotNull(searchResult);
        Assert.assertTrue(searchResult.isSucceeded());
        Assert.assertTrue(searchResult.getTotal() > 0);
        Assert.assertTrue(searchResult.getHits(Map.class).size() > 0);
    }

    @Test
    public void test_G2_multiSearch() {
        List<Search> searches = Lists.newArrayList();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery("德国", "title", "brand")).filter(QueryBuilders.termQuery("sendWay", 1));
        searchSourceBuilder.sort("id", SortOrder.DESC);
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(index).addType(type).build();
        searches.add(search);
        SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder();
        QueryBuilder queryBuilder2 = QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery("德国", "title", "brand")).filter(QueryBuilders.termQuery("sendWay", 1));
        searchSourceBuilder2.sort("id", SortOrder.DESC);
        searchSourceBuilder2.query(queryBuilder);
        searchSourceBuilder2.from(7);
        searchSourceBuilder2.size(9);
        Search search2 = new Search.Builder(searchSourceBuilder2.toString()).addIndex(index).addType(type).build();
        searches.add(search2);
        MultiSearchResult multiSearchResult = client.multiSearch(searches);
        Assert.assertNotNull(multiSearchResult);
        Assert.assertTrue(multiSearchResult.isSucceeded());
        Assert.assertTrue(multiSearchResult.getResponses().size() > 0);
    }

    @Test
    public void test_X_updateByQuery() {
//        UpdateByQueryRequestBuilder updateByQueryRequestBuilder = new UpdateByQueryRequestBuilder();
//        client.updateByQuery();
    }

    /**
     * @see <a href="https://github.com/searchbox-io/Jest/blob/master/jest/src/test/java/io/searchbox/core/SearchScrollIntegrationTest.java">search scroll test</a>
     */
    @Test
    public void test_H_scroll() {

    }

    @After
    public void after() {
        client.close();
    }

}
