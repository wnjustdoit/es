package com.caiya.elasticsearch.core;

import com.caiya.elasticsearch.ElasticSearchApi;

/**
 * Interface that specified a basic set of Elasticsearch operations, implemented by {@link ElasticSearchTemplate}. Not often used but a
 * useful option for extensibility and testability (as it can be easily mocked or stubbed).
 *
 * @author wangnan
 * @since 1.0
 */
public interface ElasticSearchOperations extends ElasticSearchApi {


}
