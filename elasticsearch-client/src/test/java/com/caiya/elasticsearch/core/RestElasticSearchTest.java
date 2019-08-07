package com.caiya.elasticsearch.core;

import com.google.common.collect.Lists;
import org.elasticsearch.action.support.WriteRequest;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.util.List;

/**
 * RestElasticSearchTest.
 *
 * @author wangnan
 * @since 1.0
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RestElasticSearchTest extends BaseElasticSearchTest {

    @Before
    public void before() {
        List<String> clusters = Lists.newArrayList("127.0.0.1:9200", "127.0.0.1:9201");
        elasticSearchClient = RestElasticSearchClientBuilder.create(clusters)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue())
                .setRefresh(false)
                .build();
    }


    @After
    public void after() {
        ((RestElasticSearchClient) elasticSearchClient).close();
    }


}
