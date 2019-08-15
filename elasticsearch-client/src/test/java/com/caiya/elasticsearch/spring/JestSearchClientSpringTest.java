package com.caiya.elasticsearch.spring;

import com.caiya.elasticsearch.jestclient.JestSearchClient;
import com.caiya.elasticsearch.jestclient.JestSearchClientTest;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

/**
 * JestSearchClientTest.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestApplication.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JestSearchClientSpringTest extends JestSearchClientTest {

    @Resource
    private JestSearchClient jestSearchClient;


    @Before
    public void before() {
        super.client = jestSearchClient;
    }

    @After
    public void after() {
    }


}
