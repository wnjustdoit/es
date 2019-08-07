package com.caiya.elasticsearch.spring.test;

import com.caiya.elasticsearch.jestclient.JestSearchClient;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import javax.annotation.Resource;

/**
 * JestSearchClientTest.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JestSearchClientTest extends BaseTest {


    @Resource
    private JestSearchClient jestSearchClient;

    private String index = "item";

    private String type = "item";

    private String id = "80003684";

    private String id1 = "80003688";

    private String id2 = "80003690";


    @Test
    public void test_A_index() {
        String jsonStr = "{\"updated\":1534471509,\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521769233717697303.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":53,\"popularity\":0,\"description\":\"这是一条描述\",\"id\":80003684,\"title\":\"德国施巴儿童洗发液150ml\"}";
        boolean result = jestSearchClient.index(index, type, id, jsonStr, true);
        Assert.assertTrue(result);
    }


}
