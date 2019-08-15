package com.caiya.elasticsearch.spring.component;

import com.caiya.elasticsearch.EsClient;
import lombok.Data;
import org.elasticsearch.action.support.WriteRequest;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * ElasticSearch属性.
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

    private EsClient.Type type = EsClient.Type.REST_HIGH_LEVEL;

    private WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.NONE;

    private boolean refresh = false;

}
