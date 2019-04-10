package com.caiya.elasticsearch.spring.test.component;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * ElasticSearch Properties conf.
 *
 * @author wangnan
 * @since 1.0
 */
@Component
@ConfigurationProperties(prefix = "es")
@Data
public class ElasticSearchProperties {

    private Map<String, String> settings;

    private List<String> clusters;


}
