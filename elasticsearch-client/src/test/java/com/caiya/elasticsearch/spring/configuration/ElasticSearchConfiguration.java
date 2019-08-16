package com.caiya.elasticsearch.spring.configuration;

import com.caiya.elasticsearch.core.ElasticSearchTemplate;
import com.caiya.elasticsearch.jestclient.JestSearchClient;
import com.caiya.elasticsearch.spring.component.ElasticSearchProperties;
import com.caiya.elasticsearch.spring.component.JestSearchProperties;
import io.searchbox.client.config.HttpClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;

/**
 * ElasticSearch配置.
 *
 * @author wangnan
 * @since 1.0
 */
@Configuration
public class ElasticSearchConfiguration {


    @Resource
    private ElasticSearchProperties elasticSearchProperties;

    @Resource
    private JestSearchProperties jestSearchProperties;


    @Bean
    public ElasticSearchTemplate elasticSearchTemplate() {
        ElasticSearchTemplate elasticSearchTemplate = new ElasticSearchTemplate();
        elasticSearchTemplate.setSettings(elasticSearchProperties.getSettings());
        elasticSearchTemplate.setClusters(elasticSearchProperties.getClusters());
        elasticSearchTemplate.setRefreshPolicy(elasticSearchProperties.getRefreshPolicy());
        elasticSearchTemplate.setRefresh(elasticSearchProperties.isRefresh());
        elasticSearchTemplate.setClientType(elasticSearchProperties.getType());
        return elasticSearchTemplate;
    }

    @Bean(destroyMethod = "close")
    public JestSearchClient jestSearchClient() {
        return JestSearchClient.builder()
                .withHttpClientConfig(new HttpClientConfig
                        .Builder(jestSearchProperties.getClusters())
                        .multiThreaded(true)
                        //Per default this implementation will create no more than 2 concurrent connections per given route
                        .defaultMaxTotalConnectionPerRoute(2)
                        // and no more 20 connections in total
                        .maxTotalConnection(20)
                        .build())
                .build();
    }


}
