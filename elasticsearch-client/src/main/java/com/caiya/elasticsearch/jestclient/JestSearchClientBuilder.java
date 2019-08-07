package com.caiya.elasticsearch.jestclient;

import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

public final class JestSearchClientBuilder {

    private JestClientFactory jestClientFactory;

    JestSearchClientBuilder() {
        jestClientFactory = new JestClientFactory();
    }

    public JestSearchClientBuilder withHttpClientConfig(HttpClientConfig httpClientConfig) {
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        return this;
    }

    public JestSearchClient build() {
        return new JestSearchClient(jestClientFactory);
    }


}
