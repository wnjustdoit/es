package com.caiya.elasticsearch.core;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * XPackTransportClientTest.
 *
 * @author wangnan
 * @since 1.0
 */
public class XPackTransportClientTest {

    @Test
    @Ignore
    public void test() throws UnknownHostException {
        TransportClient client = new PreBuiltXPackTransportClient(Settings.builder()//
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", true)
                .put("xpack.security.transport.ssl.enabled", false)
                .put("xpack.security.user", "admin:123456")
                .build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        GetResponse response = client.prepareGet("item", "item", "80003684").get();
        client.close();
        Map<String, Object> row = response.getSource();
        Assert.assertNotNull(row);
        Assert.assertTrue(row.size() > 0);
    }

}
