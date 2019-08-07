package com.caiya.elasticsearch.spring.test.component;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * JestSearch属性.
 *
 * @author wangnan
 * @since 1.0
 */
@Component
@ConfigurationProperties(prefix = "jest")
@Data
public class JestSearchProperties {

    private List<String> clusters;

}
