package com.caiya.elasticsearch.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author wangnan
 * @since 1.0
 */
@SpringBootApplication(scanBasePackages = "com.caiya")
public class TestApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }

}
