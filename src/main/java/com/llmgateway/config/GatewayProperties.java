package com.llmgateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    private Admin admin = new Admin();
    private Upstream upstream = new Upstream();

    @Data
    public static class Admin {
        private String masterKey = "gw-master-secret-key-change-in-prod";
    }

    @Data
    public static class Upstream {
        private String baseUrl = "http://localhost:8089/v1";
        private String apiKey = "";
        private int connectTimeoutMs = 5000;
        private int readTimeoutMs = 60000;
    }
}

