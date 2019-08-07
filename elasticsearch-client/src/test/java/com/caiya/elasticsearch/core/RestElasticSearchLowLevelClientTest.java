package com.caiya.elasticsearch.core;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.junit.*;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RestElasticSearchLowLevelClientTest {

    private static final Logger logger = LoggerFactory.getLogger(RestElasticSearchLowLevelClientTest.class);

    private RestClient restClient;

    @Before
    public void before() {
        List<String> clusters = Lists.newArrayList("127.0.0.1:9200", "127.0.0.1:9201");
        restClient = RestElasticSearchClientBuilder.create(clusters)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue())
                .setRefresh(false)
                .buildOriginalLowLevelClient();
    }

    @Test
    public void test_A() throws IOException {
//        Request request = new Request("GET", "/", Maps.newHashMap(), null);
        // 仅做演示参数
        Map<String, String> params = Maps.newHashMap();
        params.put("pretty", "true");
        // 仅做演示参数
        HttpEntity entity = new NStringEntity(
                "{\"json\":\"text\"}",
                ContentType.APPLICATION_JSON);
        Response response = restClient.performRequest("GET", "/", params, entity);
        RequestLine requestLine = response.getRequestLine();
        HttpHost host = response.getHost();
        int statusCode = response.getStatusLine().getStatusCode();
        Header[] headers = response.getHeaders();
        String responseBody = EntityUtils.toString(response.getEntity());
        Assert.assertEquals(200, statusCode);
    }

    @Test
    public void test_B_Async() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        restClient.performRequestAsync("GET", "/", new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                countDownLatch.countDown();
                logger.info("request perform success");
                // do business
                try {
                    logger.info(EntityUtils.toString(response.getEntity()));
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }

            }

            @Override
            public void onFailure(Exception exception) {
                countDownLatch.countDown();
                logger.error("request perform success", exception);
                // do business

            }
        });
        logger.info("I'm main thread.");
        countDownLatch.await(10, TimeUnit.SECONDS);
    }


    @After
    public void after() {
        try {
            restClient.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }


}
