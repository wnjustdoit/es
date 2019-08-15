package com.caiya.elasticsearch.core;

import com.caiya.elasticsearch.BaseElasticSearchTest;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.caiya.elasticsearch.EsClient;
import org.elasticsearch.action.support.WriteRequest;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.Map;

/**
 * ElasticSearchTemplateTest.
 *
 * @author wangnan
 * @since 1.0
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ElasticSearchTemplateTest extends BaseElasticSearchTest {

    ElasticSearchTemplate elasticSearchTemplate;

    /**
     * 注意tcp端口和http端口不一样，这个由使用者自己判断根据所选用的端口调用对应的clientType，反之亦然
     */
    @Before
    public void before() {
        Map<String, String> settings = Maps.newHashMap();
        settings.put("cluster.name", "elasticsearch");
        settings.put("xpack.security.user", "elastic:changeme");
        List<String> clusters = Lists.newArrayList("127.0.0.1:9200", "127.0.0.1:9201");// http通信端口 EsClient.Type.REST_HIGH_LEVEL
//        List<String> clusters = Lists.newArrayList("127.0.0.1:9300", "127.0.0.1:9301");// tcp通信端口 EsClient.Type.TRANSPORT
        elasticSearchTemplate = new ElasticSearchTemplate();
        elasticSearchTemplate.setSettings(settings);
        elasticSearchTemplate.setClusters(clusters);
        elasticSearchTemplate.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        elasticSearchTemplate.setRefresh(false);
        elasticSearchTemplate.setClientType(EsClient.Type.REST_HIGH_LEVEL);
        super.elasticSearchClient = elasticSearchTemplate;// 复用测试用例
    }


    @After
    public void after() {
        // do nothing
    }


    /**
     * 兼容处理
     */
    @Override
    public void test_Z2_deleteByQuery() {
        if (elasticSearchTemplate.getClientType().equals(EsClient.Type.TRANSPORT)) {
            super.test_Z2_deleteByQuery();
        }
    }
}
