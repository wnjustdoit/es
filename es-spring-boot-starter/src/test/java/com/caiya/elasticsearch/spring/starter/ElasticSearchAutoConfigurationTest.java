package com.caiya.elasticsearch.spring.starter;

import com.caiya.elasticsearch.core.ElasticSearchTemplate;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ElasticSearchApplication.class)
public class ElasticSearchAutoConfigurationTest {

    @Resource
    private ElasticSearchTemplate elasticSearchTemplate;

    private String index = "item";

    private String type = "item";

    private String id = "80003684";

    @Test
    public void test_A_index() {
        String jsonStr = "{\"updated\":1534471509,\"imgMain\":\"https://mm-jinhuo.oss-cn-shanghai.aliyuncs.com/cms-address-1521769233717697303.jpg\",\"isShowPrice\":1,\"itemPurchaseType\":0,\"sendWay\":1,\"price\":53,\"popularity\":0,\"description\":\"\",\"id\":80003684,\"title\":\"德国施巴儿童洗发液150ml\"}";
        boolean result = elasticSearchTemplate.index(index, type, id, jsonStr);
        Assert.assertTrue(result);
    }

    @Test
    public void test_B_get() {
        Map<String, Object> result = elasticSearchTemplate.get(index, type, id);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }


}
