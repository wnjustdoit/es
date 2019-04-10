package com.caiya.elasticsearch.core;

import com.caiya.elasticsearch.EsClient;

/**
 * Callback interface for ElasticSearch 'low level' code. To be used with {@link ElasticSearchTemplate} execution methods, often as
 * anonymous classes within a method implementation. Usually, used for chaining several operations together.
 *
 * @author wangnan
 * @since 1.0
 */
public interface ElasticSearchCallback<T> {

    T doInElasticSearch(EsClient client);

}
