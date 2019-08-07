package com.caiya.elasticsearch.core;

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
        Map<String, String> kvs = Maps.newHashMap();
        kvs.put("cluster.name", "elasticsearch");
        kvs.put("client.transport.sniff", "true");
        kvs.put("xpack.security.user", "elastic:changeme");
        List<String> clusters = Lists.newArrayList("127.0.0.1:9300", "127.0.0.1:9301");
        elasticSearchClient = ElasticSearchClientBuilder.create(kvs, clusters)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue())
                .setRefresh(false)
                .build();
    }


    @After
    public void after() {
        ((ElasticSearchClient) elasticSearchClient).close();
    }


}
