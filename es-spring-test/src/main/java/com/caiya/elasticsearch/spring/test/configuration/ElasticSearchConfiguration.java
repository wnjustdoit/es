package com.caiya.elasticsearch.spring.test.configuration;

import com.caiya.elasticsearch.EsClient;
import com.caiya.elasticsearch.core.ElasticSearchTemplate;
import com.caiya.elasticsearch.spring.test.component.ElasticSearchProperties;
import org.elasticsearch.action.support.WriteRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;

/**
 * ElasticSearch 配置类.
 *
 * @author wangnan
 * @since 1.0
 */
@Configuration
public class ElasticSearchConfiguration {


    @Resource
    private ElasticSearchProperties elasticSearchProperties;


    @Bean
    public ElasticSearchTemplate elasticSearchTemplate() {
        ElasticSearchTemplate elasticSearchTemplate = new ElasticSearchTemplate();
        elasticSearchTemplate.setSettings(elasticSearchProperties.getSettings());
        elasticSearchTemplate.setClusters(elasticSearchProperties.getClusters());
        elasticSearchTemplate.setRefresh(WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue());
        elasticSearchTemplate.setClientType(EsClient.Type.TRANSPORT);
        return elasticSearchTemplate;
    }


}
