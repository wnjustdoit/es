## elasticsearch client
---
#### 实现说明
* 基于elasticsearch官方提供的api、spring-data-elasticsearch、Jest集成的es客户端，客户端类型：TransportClient（即将废弃）、Java High Level REST Client（可获得Low Level）、Jest HTTP Rest Client；
* es服务端版本从V6.3.0开始（XPACK默认安装、支持elastic-sql）；
* low-level REST client和所有服务端es版本兼容，只实现低级api可单独引入maven坐标：

```xml
<dependency>
    <groupId>org.elasticsearch.client</groupId>
    <artifactId>elasticsearch-rest-client</artifactId>
    <version>${latest.version:7.3.0}</version>
</dependency>
```

* high-level client api基于low-level client api，从高级客户端获取低级客户端：

```java
/**
* {@link com.caiya.elasticsearch.core.RestElasticSearchClientBuilder#buildOriginalLowLevelClient }
*/
```

* 不需要XPACK验证的客户端，直接引入maven坐标：

```xml
<dependency>
    <groupId>org.elasticsearch.client</groupId>
    <artifactId>transport</artifactId>
    <version>6.3.0</version>
</dependency>
```

* 需要XPACK验证的客户端，除了引入对应的maven坐标外，还需要引入es官方仓库地址：

```xml
    <repositories>
        <!-- add the elasticsearch repo -->
        <repository>
            <id>elasticsearch-releases</id>
            <url>https://artifacts.elastic.co/maven</url>
            <releases>
                <enabled>true</enabled>
            </releases>
            <snapshots>
                <enabled>false</enabled>
            </snapshots>
        </repository>
    </repositories>
```






