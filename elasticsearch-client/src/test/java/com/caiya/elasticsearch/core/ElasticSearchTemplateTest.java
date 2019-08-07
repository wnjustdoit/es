package com.caiya.elasticsearch.core;

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

    /**
     * 注意tcp端口和http端口不一样，这个由使用者自己判断根据所选用的端口调用对应的clientType，反之亦然
     */
    @Before
    public void before() {
        Map<String, String> kvs = Maps.newHashMap();
        kvs.put("cluster.name", "elasticsearch");
        kvs.put("client.transport.sniff", "true");
        kvs.put("xpack.security.user", "elastic:changeme");
        List<String> clusters = Lists.newArrayList("127.0.0.1:9200", "127.0.0.1:9201");
        ElasticSearchTemplate elasticSearchTemplate = new ElasticSearchTemplate();
        elasticSearchTemplate.setSettings(kvs);
        elasticSearchTemplate.setClusters(clusters);
        elasticSearchTemplate.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        elasticSearchTemplate.setRefresh(false);
        elasticSearchTemplate.setClientType(EsClient.Type.REST_HIGH_LEVEL);
        super.elasticSearchClient = elasticSearchTemplate;// 复用测试用例
    }


    @After
    public void after() {
        // do nothing
    }


}
