package com.caiya.elasticsearch;

/**
 * ElasticsSearch Exception.
 *
 * @author wangnan
 * @since 1.0
 */
public class ElasticSearchException extends RuntimeException {


    private static final long serialVersionUID = 2656881053106255218L;

    public ElasticSearchException(String msg) {
        super(msg);
    }

    public ElasticSearchException(Exception e) {
        super(e);
    }

    public ElasticSearchException(String msg, Exception e) {
        super(msg, e);
    }


}
