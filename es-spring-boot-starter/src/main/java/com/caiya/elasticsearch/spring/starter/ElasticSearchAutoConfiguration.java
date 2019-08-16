package com.caiya.elasticsearch.spring.starter;

import com.caiya.elasticsearch.core.ElasticSearchClient;
import com.caiya.elasticsearch.core.ElasticSearchTemplate;
import com.caiya.elasticsearch.core.RestElasticSearchClient;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchDataAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass({ElasticSearchClient.class, RestElasticSearchClient.class})
@ConditionalOnProperty(prefix = "es", name = "enabled", havingValue = "true", matchIfMissing = true)
@AutoConfigureBefore({ElasticsearchAutoConfiguration.class, ElasticsearchDataAutoConfiguration.class})
@EnableConfigurationProperties(ElasticSearchProperties.class)
public class ElasticSearchAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(ElasticSearchTemplate.class)
    public ElasticSearchTemplate elasticSearchTemplate(ElasticSearchProperties elasticSearchProperties) {
        ElasticSearchTemplate elasticSearchTemplate = new ElasticSearchTemplate();
        elasticSearchTemplate.setSettings(elasticSearchProperties.getSettings());
        elasticSearchTemplate.setClusters(elasticSearchProperties.getClusters());
        elasticSearchTemplate.setRefreshPolicy(elasticSearchProperties.getRefreshPolicy());
        elasticSearchTemplate.setRefresh(elasticSearchProperties.isRefresh());
        elasticSearchTemplate.setClientType(elasticSearchProperties.getType());
        return elasticSearchTemplate;
    }

}
