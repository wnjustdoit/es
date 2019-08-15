package com.caiya.elasticsearch.core;

import com.caiya.elasticsearch.BaseElasticSearchTest;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.action.support.WriteRequest;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.Map;

/**
 * ElasticSearchTest.
 *
 * @author wangnan
 * @since 1.0
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ElasticSearchTest extends BaseElasticSearchTest {

    @Before
    public void before() {
        Map<String, String> settings = Maps.newHashMap();
        settings.put("cluster.name", "elasticsearch");
        settings.put("xpack.security.user", "elastic:changeme");
        List<String> clusters = Lists.newArrayList("127.0.0.1:9300", "127.0.0.1:9301");
        elasticSearchClient = ElasticSearchClientBuilder.create(settings, clusters)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE.getValue())
                .setRefresh(false)
                .build();
    }


    @After
    public void after() {
        ((ElasticSearchClient) elasticSearchClient).close();
    }


}
