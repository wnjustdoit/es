package com.caiya.elasticsearch.spring;

import com.caiya.elasticsearch.BaseElasticSearchTest;
import com.caiya.elasticsearch.EsClient;
import com.caiya.elasticsearch.core.ElasticSearchTemplate;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

/**
 * ElasticSearchTemplateTest.
 *
 * @author wangnan
 * @since 1.0
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestApplication.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ElasticSearchTemplateTest extends BaseElasticSearchTest {

    @Resource
    private ElasticSearchTemplate elasticSearchTemplate;

    private String index = "item";

    private String type = "item";

    private String id = "80003684";

    private String id1 = "80003688";

    private String id2 = "80003690";

    @Before
    public void before() {
        super.elasticSearchClient = elasticSearchTemplate;// 方便copy测试用例
    }


    @After
    public void after() {
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
