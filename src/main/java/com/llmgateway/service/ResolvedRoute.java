package com.llmgateway.service;

import com.llmgateway.config.GatewayProperties.RouteTarget;
import com.llmgateway.provider.LlmProvider;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ResolvedRoute {

    private String requestedAlias;
    private LlmProvider primaryProvider;
    private String targetModel;
    private List<RouteTarget> allTargets;
}

